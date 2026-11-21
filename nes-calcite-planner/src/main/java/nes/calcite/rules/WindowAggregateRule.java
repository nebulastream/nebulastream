package nes.calcite.rules;

import org.apache.calcite.plan.RelRule;
import nes.calcite.sql.NesFunctions;
import nes.calcite.logical.NesWindowAggregate;
import java.util.ArrayList;
import java.util.List;
import nes.calcite.proto.WindowAggregate;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;

/**
 * Rewrites {@code Aggregate([Project]([Filter](TUMBLE|HOP(input))))} grouped by window_start and/or window_end into
 * {@code Project(NesWindowAggregate(TUMBLE|HOP(Project([Filter](input)))))}.
 *
 * <p>The lower Project computes the grouping keys and aggregate arguments as plain columns, because NebulaStream
 * aggregates over fields only. The aggregation groups by window_start and window_end, and the upper Project restores the
 * Aggregate's column order and names. Anything the rule
 * does not recognize is left untouched and later fails the Substrait conversion with "Unable to handle node".
 */
public final class WindowAggregateRule extends RelRule<RuleConfig> {
    public static final RelOptRule INSTANCE = RuleConfig.of("NesWindowAggregateRule",
        b -> b.operand(Aggregate.class).anyInputs(), WindowAggregateRule::new).toRule();

    /** Markers in the column mapping for the two window columns; all other values index the lower Project. */
    private static final int WINDOW_START = -1;
    private static final int WINDOW_END = -2;

    private WindowAggregateRule(RuleConfig config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Aggregate agg = call.rel(0);
        if (agg instanceof NesWindowAggregate || agg.getGroupType() != Aggregate.Group.SIMPLE) {
            return;
        }

        RelNode node = strip(agg.getInput());
        List<RexNode> exprs = null;
        if (node instanceof Project project) {
            exprs = project.getProjects();
            node = strip(project.getInput());
        }
        RexNode filter = null;
        if (node instanceof Filter f) {
            filter = f.getCondition();
            node = strip(f.getInput());
        }
        if (!(node instanceof TableFunctionScan tfs) || tfs.getInputs().isEmpty()) {
            return;
        }

        WindowAggregate.Builder spec = WindowAggregate.newBuilder();
        if (!parseWindow((RexCall) tfs.getCall(), spec)) {
            return;
        }

        RelNode source = windowInput(tfs);
        int sourceWidth = source.getRowType().getFieldCount(); // window_start = sourceWidth, window_end = sourceWidth + 1
        if (filter != null && referencesWindow(filter, sourceWidth)) {
            return;
        }
        if (exprs == null) {
            exprs = new ArrayList<>();
            for (int i = 0; i < tfs.getRowType().getFieldCount(); i++) {
                exprs.add(RexInputRef.of(i, tfs.getRowType()));
            }
        }

        /// Columns of the lower Project. TUMBLE/HOP pass their input through unchanged at the same positions, so
        /// expressions over the window output are valid over the source as long as they avoid the window columns.
        List<RexNode> below = new ArrayList<>();
        int timeField = indexOf(below, RexInputRef.of(spec.getTimeField(), source.getRowType()));

        /// Per Aggregate output: a column of the lower Project, WINDOW_START/WINDOW_END, or for an aggregate the index of
        /// its call (in aggCalls), or a literal.
        List<Object> outputs = new ArrayList<>();
        boolean groupsByWindow = false;
        for (int group : agg.getGroupSet()) {
            Integer column = mapColumn(exprs.get(group), sourceWidth, below);
            if (column == null) {
                return;
            }
            groupsByWindow |= column < 0;
            outputs.add(column);
        }
        if (!groupsByWindow) {
            return;
        }

        List<AggregateCall> aggCalls = new ArrayList<>();
        for (AggregateCall aggCall : agg.getAggCallList()) {
            /// LITERAL_AGG(true) marks matches in Calcite's NOT IN/NOT EXISTS rewrite: a constant, not an aggregation.
            if (aggCall.getAggregation().getKind() == SqlKind.LITERAL_AGG && aggCall.rexList.size() == 1) {
                outputs.add(aggCall.rexList.get(0));
                continue;
            }
            String function = toFunction(aggCall);
            if (function == null || (aggCall.getArgList().isEmpty() && !function.equals("COUNT"))) {
                return;
            }
            List<Integer> arguments = List.of();
            if (!aggCall.getArgList().isEmpty()) {
                Integer column = mapColumn(exprs.get(aggCall.getArgList().get(0)), sourceWidth, below);
                if (column == null || column < 0) {
                    return;
                }
                arguments = List.of(column);
            }
            outputs.add(new int[] {aggCalls.size()});
            aggCalls.add(aggCall.withArgList(arguments));
        }

        RelBuilder builder = call.builder();
        builder.push(source);
        if (filter != null) {
            builder.filter(filter);
        }
        builder.project(below);
        RelNode rows = builder.build();
        int width = below.size();
        /// Grouped by both window columns (window_start determines window_end), whichever the query groups by.
        ImmutableBitSet.Builder groupSet = ImmutableBitSet.builder().set(width).set(width + 1);
        outputs.stream().filter(o -> o instanceof Integer column && column >= 0).forEach(o -> groupSet.set((Integer) o));
        ImmutableBitSet groups = groupSet.build();
        /// The window columns have the names of the Aggregate's columns (if it has them), as its other columns.
        List<String> names = new ArrayList<>(List.of("window_start", "window_end"));
        for (int i = 0; i < agg.getGroupCount(); i++) {
            if (outputs.get(i) instanceof Integer column && column < 0) {
                names.set(column == WINDOW_START ? 0 : 1, agg.getRowType().getFieldNames().get(i));
            }
        }
        RelNode window = new NesWindowAggregate(agg.getCluster(), agg.getTraitSet(),
            NesWindowAggregate.windowFunction(tfs, rows, timeField, names.get(0), names.get(1)), groups, aggCalls, false);

        builder.push(window);
        List<RexNode> reorder = new ArrayList<>();
        for (Object output : outputs) {
            if (output instanceof Integer column) {
                int group = column == WINDOW_START ? width : column == WINDOW_END ? width + 1 : column;
                reorder.add(builder.field(groups.indexOf(group)));
            } else if (output instanceof int[] aggregate) {
                reorder.add(builder.field(groups.cardinality() + aggregate[0]));
            } else {
                reorder.add((RexNode) output);
            }
        }
        builder.project(reorder, agg.getRowType().getFieldNames(), true);
        call.transformTo(builder.build());
    }

    /**
     * The rows the window function reads. Calcite passes a UNION ALL argument (TUMBLE((SELECT ... UNION ALL SELECT ...),
     * ...)) as one input per branch; they are the union's rows.
     */
    static RelNode windowInput(TableFunctionScan tfs) {
        List<RelNode> inputs = tfs.getInputs().stream().map(WindowAggregateRule::strip).toList();
        return inputs.size() == 1 ? inputs.get(0) : org.apache.calcite.rel.logical.LogicalUnion.create(inputs, true);
    }

    /** Records Calcite's result type, to which the converter casts the aggregation's result. */
    public static WindowAggregate.Aggregate.Builder withType(WindowAggregate.Aggregate.Builder aggregate, RelDataType type) {
        return aggregate.setType(type.getSqlTypeName().getName()).setNullable(type.isNullable());
    }

    /** TUMBLE(DESCRIPTOR(ts), size) or HOP(DESCRIPTOR(ts), slide, size); offsets are not supported. */
    public static boolean parseWindow(RexCall invocation, WindowAggregate.Builder spec) {
        List<RexNode> operands = invocation.getOperands();
        /// The table functions have kind OTHER_FUNCTION; SqlKind.TUMBLE/HOP are the old GROUP BY window functions.
        String name = invocation.getOperator().getName();
        if (name.equals("TUMBLE") && operands.size() == 2) {
            spec.setKind(WindowAggregate.Kind.TUMBLING).setSizeMs(millis(operands.get(1))).setSlideMs(millis(operands.get(1)));
        } else if (name.equals("HOP") && operands.size() == 3) {
            spec.setKind(WindowAggregate.Kind.SLIDING).setSlideMs(millis(operands.get(1))).setSizeMs(millis(operands.get(2)));
        } else {
            return false;
        }
        if (!(operands.get(0) instanceof RexCall descriptor)
            || descriptor.getOperands().size() != 1
            || !(descriptor.getOperands().get(0) instanceof RexInputRef timeField)) {
            return false;
        }
        spec.setTimeField(timeField.getIndex());
        return true;
    }

    /** Interval literals are stored in milliseconds. */
    private static long millis(RexNode interval) {
        return ((RexLiteral) interval).getValueAs(Long.class);
    }

    /** WINDOW_START / WINDOW_END, an index into {@code below}, or null if the expression mixes in window columns. */
    private static Integer mapColumn(RexNode expr, int sourceWidth, List<RexNode> below) {
        if (expr instanceof RexInputRef ref && ref.getIndex() >= sourceWidth) {
            return ref.getIndex() == sourceWidth ? WINDOW_START : WINDOW_END;
        }
        if (referencesWindow(expr, sourceWidth)) {
            return null;
        }
        return indexOf(below, expr);
    }

    private static boolean referencesWindow(RexNode expr, int sourceWidth) {
        return RelOptUtil.InputFinder.bits(expr).asList().stream().anyMatch(i -> i >= sourceWidth);
    }

    private static int indexOf(List<RexNode> columns, RexNode expr) {
        int index = columns.indexOf(expr);
        if (index < 0) {
            columns.add(expr);
            index = columns.size() - 1;
        }
        return index;
    }

    /** The name of our aggregation, or null if the engine has none for the call. */
    public static String toFunction(AggregateCall call) {
        if (call.isDistinct() || call.isApproximate() || call.hasFilter() || call.getArgList().size() > 1
            || !call.getCollation().getFieldCollations().isEmpty()) {
            return null;
        }
        return switch (call.getAggregation().getKind()) {
            case SUM, SUM0 -> "SUM";
            case COUNT, MIN, MAX, AVG -> call.getAggregation().getKind().name();
            default -> call.getAggregation() instanceof NesFunctions.Aggregation ? call.getAggregation().getName() : null;
        };
    }

    private static RelNode strip(RelNode node) {
        return node instanceof HepRelVertex vertex ? vertex.getCurrentRel() : node;
    }
}

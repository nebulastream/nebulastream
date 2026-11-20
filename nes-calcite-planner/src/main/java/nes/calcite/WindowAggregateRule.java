package nes.calcite;

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
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;

/**
 * Rewrites {@code Aggregate([Project]([Filter](TUMBLE|HOP(input))))} grouped by window_start and/or window_end into
 * {@code Project(NesWindowAggregate(Project([Filter](input))))}.
 *
 * <p>The lower Project computes the grouping keys and aggregate arguments as plain columns, because NebulaStream
 * aggregates over fields only. The upper Project restores the Aggregate's column order and names. Anything the rule
 * does not recognize is left untouched and later fails the Substrait conversion with "Unable to handle node".
 */
final class WindowAggregateRule extends RelOptRule {
    static final WindowAggregateRule INSTANCE = new WindowAggregateRule();

    /** Markers in the column mapping for the two window columns; all other values index the lower Project. */
    private static final int WINDOW_START = -1;
    private static final int WINDOW_END = -2;

    @SuppressWarnings("deprecation")
    private WindowAggregateRule() {
        super(operand(Aggregate.class, any()), "NesWindowAggregateRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Aggregate agg = call.rel(0);
        if (agg.getGroupType() != Aggregate.Group.SIMPLE) {
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
        spec.setTimeField(indexOf(below, RexInputRef.of(spec.getTimeField(), source.getRowType())));

        boolean groupsByWindow = false;
        List<RelDataTypeField> outputFields = agg.getRowType().getFieldList();
        RelDataTypeFactory.Builder rowType = agg.getCluster().getTypeFactory().builder();
        rowType.add("START", tfs.getRowType().getFieldList().get(sourceWidth).getType());
        rowType.add("END", tfs.getRowType().getFieldList().get(sourceWidth + 1).getType());
        List<Integer> outputOrder = new ArrayList<>(); // position in NesWindowAggregate output, per Aggregate output
        int outputIndex = 0;
        for (int group : agg.getGroupSet()) {
            Integer column = mapColumn(exprs.get(group), sourceWidth, below);
            if (column == null) {
                return;
            }
            if (column < 0) {
                groupsByWindow = true;
                outputOrder.add(column == WINDOW_START ? 0 : 1);
            } else {
                spec.addGroupKeys(column);
                outputOrder.add(2 + spec.getGroupKeysCount() - 1);
                rowType.add(outputFields.get(outputIndex));
            }
            outputIndex++;
        }
        if (!groupsByWindow) {
            return;
        }

        List<RexNode> literals = new ArrayList<>(); // outputOrder entry -1 - i is literals[i]
        for (AggregateCall aggCall : agg.getAggCallList()) {
            /// LITERAL_AGG(true) marks matches in Calcite's NOT IN/NOT EXISTS rewrite: a constant, not an aggregation.
            if (aggCall.getAggregation().getKind() == SqlKind.LITERAL_AGG && aggCall.rexList.size() == 1) {
                outputOrder.add(-1 - literals.size());
                literals.add(aggCall.rexList.get(0));
                outputIndex++;
                continue;
            }
            String function = toFunction(aggCall);
            if (function == null) {
                return;
            }
            WindowAggregate.Aggregate.Builder aggregate = WindowAggregate.Aggregate.newBuilder().setFunction(function);
            if (aggCall.getArgList().isEmpty()) {
                if (!function.equals("COUNT")) {
                    return;
                }
            } else {
                Integer column = mapColumn(exprs.get(aggCall.getArgList().get(0)), sourceWidth, below);
                if (column == null || column < 0) {
                    return;
                }
                aggregate.setField(column);
            }
            spec.addAggregates(withType(aggregate, outputFields.get(outputIndex).getType()).setName(outputFields.get(outputIndex).getName()));
            outputOrder.add(2 + spec.getGroupKeysCount() + spec.getAggregatesCount() - 1);
            rowType.add(outputFields.get(outputIndex++));
        }

        addCountIfEmpty(spec, rowType, agg.getCluster().getTypeFactory());

        RelBuilder builder = call.builder();
        builder.push(source);
        if (filter != null) {
            builder.filter(filter);
        }
        builder.project(below);
        RelNode window = new NesWindowAggregate(
            agg.getCluster(), agg.getTraitSet(), builder.build(), spec.build(), rowType.build());

        builder.push(window);
        List<RexNode> reorder = outputOrder.stream().map(i -> i >= 0 ? builder.field(i) : literals.get(-1 - i)).toList();
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

    /**
     * Our aggregation needs at least one aggregate: one without (Calcite's deduplication for IN/EXISTS) gets a COUNT(*),
     * which the projection on top leaves out.
     */
    static void addCountIfEmpty(WindowAggregate.Builder spec, RelDataTypeFactory.Builder rowType, RelDataTypeFactory types) {
        if (spec.getAggregatesCount() > 0) {
            return;
        }
        RelDataType count = types.createSqlType(org.apache.calcite.sql.type.SqlTypeName.BIGINT);
        spec.addAggregates(withType(WindowAggregate.Aggregate.newBuilder().setFunction("COUNT"), count)
            .setName("$f_count"));
        rowType.add("$f_count", count);
    }

    /** Records Calcite's result type, to which the converter casts the aggregation's result. */
    static WindowAggregate.Aggregate.Builder withType(WindowAggregate.Aggregate.Builder aggregate, RelDataType type) {
        return aggregate.setType(type.getSqlTypeName().getName()).setNullable(type.isNullable());
    }

    /** TUMBLE(DESCRIPTOR(ts), size) or HOP(DESCRIPTOR(ts), slide, size); offsets are not supported. */
    static boolean parseWindow(RexCall invocation, WindowAggregate.Builder spec) {
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
    static String toFunction(AggregateCall call) {
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

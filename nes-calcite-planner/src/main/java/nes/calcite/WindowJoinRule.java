package nes.calcite;

import java.util.ArrayList;
import java.util.List;
import nes.calcite.proto.WindowAggregate;
import nes.calcite.proto.WindowJoin;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;

/**
 * Rewrites {@code Join(TUMBLE|HOP(left), TUMBLE|HOP(right)) ON left.window_start = right.window_start AND ...} into
 * {@code Project(NesWindowJoin(left, right))}. Both sides must use the same window. A side is a stream (TUMBLE/HOP,
 * possibly below column-selecting projections and filters on non-window columns) or the result of a tumbling windowed
 * aggregation; the latter also covers IN/EXISTS subqueries, which Calcite turns into joins with a deduplicating
 * aggregation.
 *
 * <p>This is also where the join implementation is decided (NebulaStream's DecideJoinTypesRule keeps a choice that
 * is already made): a hash join on the leftField = rightField keys of the condition, with the rest as residual, if
 * there is at least one key; a nested-loop join otherwise.
 */
final class WindowJoinRule extends RelOptRule {
    static final WindowJoinRule INSTANCE = new WindowJoinRule();

    @SuppressWarnings("deprecation")
    private WindowJoinRule() {
        super(operand(Join.class, any()), "NesWindowJoinRule");
    }

    /**
     * One join input. Either a stream: column-selecting projections and filters over TUMBLE/HOP, whose source becomes
     * the NesWindowJoin input. Or an aggregated stream: any projections and filters over a tumbling
     * NesWindowAggregate, used as a whole with its window_start as event time (correct because each result row of
     * window [s, e) has window_start s and falls into exactly that window again).
     */
    private static final class Side {
        RelNode input;
        List<RexNode> filters = new ArrayList<>(); // over input
        WindowAggregate.Builder window = WindowAggregate.newBuilder(); // time field indexes input
        int[] inputColumn; // join-input column -> input column, -1 for the window columns of a stream
        int[] windowColumn; // join-input column -> 0 (window_start), 1 (window_end) or -1
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Join join = call.rel(0);
        Side left = describe(strip(join.getLeft()));
        Side right = describe(strip(join.getRight()));
        if (left == null || right == null
            || left.window.getKind() != right.window.getKind()
            || left.window.getSizeMs() != right.window.getSizeMs()
            || left.window.getSlideMs() != right.window.getSlideMs()) {
            return;
        }
        WindowJoin.Type type = switch (join.getJoinType()) {
            case INNER -> WindowJoin.Type.INNER;
            case LEFT -> WindowJoin.Type.LEFT;
            case RIGHT -> WindowJoin.Type.RIGHT;
            case FULL -> WindowJoin.Type.FULL;
            default -> null;
        };
        if (type == null) {
            return;
        }

        /// Join-condition column -> column of [window_start, window_end, left input..., right input...], i.e. the
        /// NesWindowJoin output. Window columns of both sides map to 0 and 1.
        int leftWidth = join.getLeft().getRowType().getFieldCount();
        int leftInputWidth = left.input.getRowType().getFieldCount();
        int[] toOutput = new int[join.getRowType().getFieldCount()];
        for (int i = 0; i < toOutput.length; i++) {
            Side side = i < leftWidth ? left : right;
            int column = i < leftWidth ? i : i - leftWidth;
            toOutput[i] = side.windowColumn[column] >= 0
                ? side.windowColumn[column]
                : 2 + (side == left ? 0 : leftInputWidth) + side.inputColumn[column];
        }

        boolean joinsOnWindow = false;
        List<RexNode> remaining = new ArrayList<>();
        for (RexNode conjunct : RelOptUtil.conjunctions(join.getCondition())) {
            if (isWindowEquality(conjunct, leftWidth, toOutput)) {
                joinsOnWindow = true;
            } else if (RelOptUtil.InputFinder.bits(conjunct).asList().stream().anyMatch(i -> toOutput[i] < 2)) {
                return; // other conditions on window columns are not supported
            } else {
                remaining.add(conjunct);
            }
        }
        if (!joinsOnWindow) {
            return;
        }

        RexBuilder rexBuilder = join.getCluster().getRexBuilder();
        RelBuilder builder = call.builder();
        RelNode leftInput = builder.push(left.input).filter(left.filters).build();
        RelNode rightInput = builder.push(right.input).filter(right.filters).build();
        RelDataType inputs = join.getCluster().getTypeFactory().builder()
            .addAll(leftInput.getRowType().getFieldList())
            .addAll(rightInput.getRowType().getFieldList())
            .build();
        RexNode condition = remaining.isEmpty() ? null : RexUtil.composeConjunction(rexBuilder, remaining).accept(new RexShuttle() {
            @Override
            public RexNode visitInputRef(RexInputRef ref) {
                return RexInputRef.of(toOutput[ref.getIndex()] - 2, inputs);
            }
        });

        WindowJoin spec = WindowJoin.newBuilder()
            .setKind(left.window.getKind())
            .setSizeMs(left.window.getSizeMs())
            .setSlideMs(left.window.getSlideMs())
            .setLeftTimeField(left.window.getTimeField())
            .setRightTimeField(right.window.getTimeField())
            .setType(type)
            .build();

        Split split = split(rexBuilder, leftInput, rightInput, condition);
        spec = spec.toBuilder().setImplementation(split.implementation()).build();
        condition = split.condition();
        List<RexNode> residual = split.residual();

        RelDataTypeFactory typeFactory = join.getCluster().getTypeFactory();
        boolean leftNullable = type == WindowJoin.Type.RIGHT || type == WindowJoin.Type.FULL;
        boolean rightNullable = type == WindowJoin.Type.LEFT || type == WindowJoin.Type.FULL;
        RelDataType windowColumn = null;
        for (int i = 0; i < toOutput.length && windowColumn == null; i++) {
            if (toOutput[i] == 0) {
                windowColumn = join.getRowType().getFieldList().get(i).getType();
            }
        }
        RelDataTypeFactory.Builder rowType = typeFactory.builder()
            .add("window_start", windowColumn)
            .add("window_end", windowColumn);
        for (RelDataTypeField field : leftInput.getRowType().getFieldList()) {
            rowType.add(field.getName(), typeFactory.createTypeWithNullability(field.getType(), leftNullable || field.getType().isNullable()));
        }
        for (RelDataTypeField field : rightInput.getRowType().getFieldList()) {
            rowType.add(field.getName(), typeFactory.createTypeWithNullability(field.getType(), rightNullable || field.getType().isNullable()));
        }
        rowType.uniquify();

        RelNode windowJoin = new NesWindowJoin(
            join.getCluster(), join.getTraitSet(), leftInput, rightInput, spec, condition, residual, rowType.build());
        builder.push(windowJoin);
        List<RexNode> outputs = new ArrayList<>();
        List<RelDataTypeField> expected = join.getRowType().getFieldList();
        for (int i = 0; i < toOutput.length; i++) {
            RexNode output = builder.field(toOutput[i]);
            /// Outer joins: Calcite types the window columns of the padded side nullable, ours never are null.
            RelDataType expectedType = expected.get(i).getType();
            outputs.add(output.getType().equals(expectedType) ? output : rexBuilder.makeAbstractCast(expectedType, output, false));
        }
        builder.project(outputs, join.getRowType().getFieldNames(), true);
        call.transformTo(builder.build());
    }

    /** left.window_start = right.window_start or left.window_end = right.window_end. */
    private static boolean isWindowEquality(RexNode conjunct, int leftWidth, int[] toOutput) {
        if (conjunct.getKind() != SqlKind.EQUALS
            || !(((RexCall) conjunct).getOperands().get(0) instanceof RexInputRef a)
            || !(((RexCall) conjunct).getOperands().get(1) instanceof RexInputRef b)) {
            return false;
        }
        boolean oneLeftOneRight = (a.getIndex() < leftWidth) != (b.getIndex() < leftWidth);
        return oneLeftOneRight && toOutput[a.getIndex()] < 2 && toOutput[a.getIndex()] == toOutput[b.getIndex()];
    }

    /** A join condition as NebulaStream's joins take it. */
    record Split(WindowJoin.Implementation implementation, RexNode condition, List<RexNode> residual) {}

    /**
     * Calcite's split of a condition over [left columns..., right columns...] into leftField = rightField keys and the
     * rest: a hash join on the keys with the rest as residual if there is at least one key, a nested-loop join over the
     * whole condition otherwise.
     */
    static Split split(RexBuilder rexBuilder, RelNode left, RelNode right, RexNode condition) {
        if (condition == null) {
            return new Split(WindowJoin.Implementation.NESTED_LOOP_JOIN, null, List.of());
        }
        JoinInfo info = JoinInfo.of(left, right, condition);
        if (info.leftKeys.isEmpty()) {
            return new Split(WindowJoin.Implementation.NESTED_LOOP_JOIN, condition, List.of());
        }
        RelDataType inputs = left.getCluster().getTypeFactory().builder()
            .addAll(left.getRowType().getFieldList())
            .addAll(right.getRowType().getFieldList())
            .build();
        int leftWidth = left.getRowType().getFieldCount();
        List<RexNode> keys = new ArrayList<>();
        for (int i = 0; i < info.leftKeys.size(); i++) {
            keys.add(rexBuilder.makeCall(
                SqlStdOperatorTable.EQUALS,
                RexInputRef.of(info.leftKeys.get(i), inputs),
                RexInputRef.of(leftWidth + info.rightKeys.get(i), inputs)));
        }
        return new Split(WindowJoin.Implementation.HASH_JOIN, RexUtil.composeConjunction(rexBuilder, keys), info.nonEquiConditions);
    }

    private static Side describe(RelNode node) {
        Side stream = describeStream(node);
        return stream != null ? stream : describeAggregated(node);
    }

    /** Column-selecting projections and filters over TUMBLE/HOP over a source. */
    private static Side describeStream(RelNode node) {
        Stream stream = traceStream(node);
        if (stream == null) {
            return null;
        }
        Side side = new Side();
        side.input = stream.source;
        side.filters = stream.filters;
        side.window = stream.window;
        side.inputColumn = new int[stream.toWindowOutput.length];
        side.windowColumn = new int[stream.toWindowOutput.length];
        for (int i = 0; i < stream.toWindowOutput.length; i++) {
            int column = stream.toWindowOutput[i];
            boolean isWindow = column >= stream.sourceWidth;
            side.inputColumn[i] = isWindow ? -1 : column;
            side.windowColumn[i] = isWindow ? column - stream.sourceWidth : -1;
        }
        return side;
    }

    /** Any projections and filters over a tumbling NesWindowAggregate. */
    private static Side describeAggregated(RelNode node) {
        int width = node.getRowType().getFieldCount();
        int[] toAggregate = new int[width]; // column -> NesWindowAggregate column, -1 if computed
        for (int i = 0; i < width; i++) {
            toAggregate[i] = i;
        }
        RelNode current = node;
        while (!(current instanceof NesWindowAggregate)) {
            if (current instanceof Filter filter) {
                current = strip(filter.getInput());
            } else if (current instanceof Project project) {
                for (int i = 0; i < width; i++) {
                    if (toAggregate[i] >= 0) {
                        RexNode expression = project.getProjects().get(toAggregate[i]);
                        toAggregate[i] = expression instanceof RexInputRef ref ? ref.getIndex() : -1;
                    }
                }
                current = strip(project.getInput());
            } else {
                return null;
            }
        }
        WindowAggregate spec = ((NesWindowAggregate) current).spec;
        if (spec.getKind() != WindowAggregate.Kind.TUMBLING) {
            return null;
        }
        Side side = new Side();
        side.input = node;
        side.inputColumn = new int[width];
        side.windowColumn = new int[width];
        int timeField = -1;
        for (int i = 0; i < width; i++) {
            side.inputColumn[i] = i;
            side.windowColumn[i] = toAggregate[i] == 0 || toAggregate[i] == 1 ? toAggregate[i] : -1;
            if (side.windowColumn[i] == 0 && timeField < 0) {
                timeField = i;
            }
        }
        if (timeField < 0) {
            return null; // window_start was projected away
        }
        side.window.setKind(spec.getKind()).setSizeMs(spec.getSizeMs()).setSlideMs(spec.getSlideMs()).setTimeField(timeField);
        return side;
    }

    private static final class Stream {
        RelNode source;
        int sourceWidth; // window_start = sourceWidth, window_end = sourceWidth + 1 (in window-function output space)
        WindowAggregate.Builder window = WindowAggregate.newBuilder();
        List<RexNode> filters = new ArrayList<>(); // over the source
        int[] toWindowOutput; // column -> window-function output column
    }

    private static Stream traceStream(RelNode node) {
        if (node instanceof TableFunctionScan tfs) {
            if (tfs.getInputs().size() != 1 || !(tfs.getCall() instanceof RexCall invocation)) {
                return null;
            }
            Stream stream = new Stream();
            if (!WindowAggregateRule.parseWindow(invocation, stream.window)) {
                return null;
            }
            stream.source = strip(tfs.getInputs().get(0));
            stream.sourceWidth = stream.source.getRowType().getFieldCount();
            stream.toWindowOutput = new int[stream.sourceWidth + 2];
            for (int i = 0; i < stream.toWindowOutput.length; i++) {
                stream.toWindowOutput[i] = i;
            }
            return stream;
        }
        if (node instanceof Project project) {
            Stream stream = traceStream(strip(project.getInput()));
            if (stream == null || !project.getProjects().stream().allMatch(e -> e instanceof RexInputRef)) {
                return null;
            }
            int[] mapping = new int[project.getProjects().size()];
            for (int i = 0; i < mapping.length; i++) {
                mapping[i] = stream.toWindowOutput[((RexInputRef) project.getProjects().get(i)).getIndex()];
            }
            stream.toWindowOutput = mapping;
            return stream;
        }
        if (node instanceof Filter filter) {
            Stream stream = traceStream(strip(filter.getInput()));
            if (stream == null) {
                return null;
            }
            int[] mapping = stream.toWindowOutput;
            RexNode condition = filter.getCondition().accept(new RexShuttle() {
                @Override
                public RexNode visitInputRef(RexInputRef ref) {
                    return new RexInputRef(mapping[ref.getIndex()], ref.getType());
                }
            });
            int sourceWidth = stream.sourceWidth;
            if (RelOptUtil.InputFinder.bits(condition).asList().stream().anyMatch(i -> i >= sourceWidth)) {
                return null;
            }
            stream.filters.add(condition);
            return stream;
        }
        return null;
    }

    private static RelNode strip(RelNode node) {
        return node instanceof HepRelVertex vertex ? vertex.getCurrentRel() : node;
    }
}

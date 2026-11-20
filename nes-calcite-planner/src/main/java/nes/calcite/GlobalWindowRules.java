package nes.calcite;

import java.util.ArrayList;
import java.util.List;
import nes.calcite.proto.WindowAggregate;
import nes.calcite.proto.WindowJoin;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.tools.RelBuilder;

/**
 * Only for finite inputs: joins and aggregations without a window become NesWindowJoin / NesWindowAggregate with a
 * GLOBAL window, one window over the whole input that fires at the end of the stream. They run after WindowAggregateRule
 * and WindowJoinRule, so everything still a plain Aggregate or Join here has no window.
 */
final class GlobalWindowRules {
    static final RelOptRule AGGREGATE = new AggregateRule();
    static final RelOptRule JOIN = new JoinRule();
    static final RelOptRule SINGLE_ROW_OUTER_JOIN = new SingleRowOuterJoinRule();

    private GlobalWindowRules() {}

    /** START and END of a GLOBAL window; never read by the query, only there to keep the layout. */
    private static RelDataTypeFactory.Builder windowColumns(RelDataTypeFactory typeFactory) {
        RelDataType bigint = typeFactory.createSqlType(SqlTypeName.BIGINT);
        return typeFactory.builder().add("START", bigint).add("END", bigint);
    }

    /**
     * The input with the event time of a GLOBAL window as an extra last column: 0 for every row, so that the window
     * [0, size) holds every row and only the end of the stream closes it. (Ingestion time depends on the wall clock:
     * nested GLOBAL operators then saw rows older than a watermark that another input had already advanced, and failed.)
     */
    private static RelNode withConstantTime(RelNode input) {
        RexBuilder rexBuilder = input.getCluster().getRexBuilder();
        List<RexNode> projects = new ArrayList<>();
        List<String> names = new ArrayList<>(input.getRowType().getFieldNames());
        for (int i = 0; i < names.size(); i++) {
            projects.add(RexInputRef.of(i, input.getRowType()));
        }
        projects.add(rexBuilder.makeAbstractCast(rexBuilder.getTypeFactory().createSqlType(SqlTypeName.UBIGINT),
            rexBuilder.makeExactLiteral(java.math.BigDecimal.ZERO), false));
        names.add("$f_time");
        return LogicalProject.create(input, List.of(), projects, SqlValidatorUtil.uniquify(names, true), java.util.Set.of());
    }

    private static final class AggregateRule extends RelOptRule {
        @SuppressWarnings("deprecation")
        AggregateRule() {
            super(operand(Aggregate.class, any()), "NesGlobalAggregateRule");
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            Aggregate agg = call.rel(0);
            if (agg.getGroupType() != Aggregate.Group.SIMPLE) {
                return;
            }
            WindowAggregate.Builder spec = WindowAggregate.newBuilder().setKind(WindowAggregate.Kind.GLOBAL);
            List<RelDataTypeField> outputFields = agg.getRowType().getFieldList();
            RelDataTypeFactory.Builder rowType = windowColumns(agg.getCluster().getTypeFactory());
            List<Object> outputs = new ArrayList<>(); // per Aggregate output: NesWindowAggregate column or a literal
            List<Integer> zeroWhenEmpty = new ArrayList<>(); // outputs of SUM0, which is 0 rather than NULL without rows

            int outputIndex = 0;
            for (int group : agg.getGroupSet()) {
                spec.addGroupKeys(group);
                outputs.add(2 + spec.getGroupKeysCount() - 1);
                rowType.add(outputFields.get(outputIndex++));
            }
            for (AggregateCall aggCall : agg.getAggCallList()) {
                if (aggCall.getAggregation().getKind() == SqlKind.LITERAL_AGG && aggCall.rexList.size() == 1) {
                    outputs.add(aggCall.rexList.get(0));
                    outputIndex++;
                    continue;
                }
                String function = WindowAggregateRule.toFunction(aggCall);
                if (function == null || (aggCall.getArgList().isEmpty() && !function.equals("COUNT"))) {
                    return;
                }
                WindowAggregate.Aggregate.Builder aggregate = WindowAggregate.Aggregate.newBuilder().setFunction(function);
                if (!aggCall.getArgList().isEmpty()) {
                    aggregate.setField(aggCall.getArgList().get(0));
                }
                spec.addAggregates(WindowAggregateRule.withType(aggregate, outputFields.get(outputIndex).getType())
                    .setName(outputFields.get(outputIndex).getName()));
                RelDataTypeField field = outputFields.get(outputIndex++);
                if (aggCall.getAggregation().getKind() == SqlKind.SUM0 && agg.getGroupSet().isEmpty()) {
                    /// Our SUM yields NULL for an empty input, which only an aggregation without GROUP BY sees.
                    zeroWhenEmpty.add(outputs.size());
                    field = new RelDataTypeFieldImpl(field.getName(), field.getIndex(),
                        agg.getCluster().getTypeFactory().createTypeWithNullability(field.getType(), true));
                }
                outputs.add(2 + agg.getGroupCount() + spec.getAggregatesCount() - 1);
                rowType.add(field);
            }

            /// SQL: an aggregation without GROUP BY returns one row even for an empty input (counts 0, all else NULL).
            if (agg.getGroupSet().isEmpty()) {
                spec.setEmitOnEmptyInput(true);
            }
            WindowAggregateRule.addCountIfEmpty(spec, rowType, agg.getCluster().getTypeFactory());
            RelNode input = withConstantTime(agg.getInput());
            spec.setTimeField(input.getRowType().getFieldCount() - 1);
            RelNode window = new NesWindowAggregate(
                agg.getCluster(), agg.getTraitSet(), input, spec.build(), rowType.build());
            RelBuilder builder = call.builder().push(window);
            List<RexNode> projections = new ArrayList<>(outputs.stream()
                .map(output -> output instanceof Integer column ? builder.field(column) : (RexNode) output)
                .toList());
            RexBuilder rexBuilder = builder.getRexBuilder();
            for (int index : zeroWhenEmpty) { // SUM0: 0 instead of NULL
                RexNode sum = projections.get(index);
                projections.set(index, rexBuilder.makeCall(SqlStdOperatorTable.CASE,
                    rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, sum), rexBuilder.makeZeroLiteral(sum.getType()), sum));
            }
            builder.project(projections, agg.getRowType().getFieldNames(), true).convert(agg.getRowType(), false);
            call.transformTo(builder.build());
        }
    }

    /**
     * A LEFT (RIGHT) join on TRUE whose right (left) side always has exactly one row is an inner join: e.g. a scalar
     * subquery such as {@code WHERE c > (SELECT avg(c) FROM t1)}, which Calcite decorrelates into such a join. Our
     * window joins need a condition for outer joins, but an inner join without one is a cross product. Calcite's row
     * count metadata knows the one-row sides, e.g. an aggregation without GROUP BY, which emits its row also for an
     * empty input here (emit_on_empty_input).
     */
    private static final class SingleRowOuterJoinRule extends RelOptRule {
        @SuppressWarnings("deprecation")
        SingleRowOuterJoinRule() {
            super(operand(Join.class, any()), "NesSingleRowOuterJoinRule");
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            Join join = call.rel(0);
            RelNode oneRowSide = switch (join.getJoinType()) {
                case LEFT -> join.getRight();
                case RIGHT -> join.getLeft();
                default -> null;
            };
            if (oneRowSide == null || !join.getCondition().isAlwaysTrue()) {
                return;
            }
            RelMetadataQuery mq = call.getMetadataQuery();
            Double min = mq.getMinRowCount(oneRowSide);
            Double max = mq.getMaxRowCount(oneRowSide);
            if (min == null || max == null || min < 1 || max > 1) {
                return;
            }
            RelNode inner = join.copy(join.getTraitSet(), join.getCondition(), join.getLeft(), join.getRight(),
                JoinRelType.INNER, join.isSemiJoinDone());
            /// The outer join's row type, whose columns from the one-row side are nullable.
            call.transformTo(call.builder().push(inner).convert(join.getRowType(), false).build());
        }
    }

    private static final class JoinRule extends RelOptRule {
        @SuppressWarnings("deprecation")
        JoinRule() {
            super(operand(Join.class, any()), "NesGlobalJoinRule");
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            Join join = call.rel(0);
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
            /// Each input gets its time column last: [left columns..., left time, right columns..., right time].
            RelNode left = withConstantTime(join.getLeft());
            RelNode right = withConstantTime(join.getRight());
            int leftWidth = join.getLeft().getRowType().getFieldCount();
            RexNode condition = join.getCondition().isAlwaysTrue() ? null : join.getCondition().accept(new RexShuttle() {
                @Override
                public RexNode visitInputRef(RexInputRef ref) {
                    return ref.getIndex() < leftWidth ? ref : new RexInputRef(ref.getIndex() + 1, ref.getType());
                }
            });
            WindowJoinRule.Split split = WindowJoinRule.split(join.getCluster().getRexBuilder(), left, right, condition);
            WindowJoin spec = WindowJoin.newBuilder()
                .setKind(WindowAggregate.Kind.GLOBAL)
                .setType(type)
                .setImplementation(split.implementation())
                .setLeftTimeField(leftWidth)
                .setRightTimeField(right.getRowType().getFieldCount() - 1)
                .build();
            RelDataTypeFactory typeFactory = join.getCluster().getTypeFactory();
            List<RelDataTypeField> fields = join.getRowType().getFieldList();
            RelDataType time = typeFactory.createSqlType(SqlTypeName.UBIGINT);
            RelDataType rowType = windowColumns(typeFactory)
                .addAll(fields.subList(0, leftWidth))
                .add("$f_time", typeFactory.createTypeWithNullability(time, type == WindowJoin.Type.RIGHT || type == WindowJoin.Type.FULL))
                .addAll(fields.subList(leftWidth, fields.size()))
                .add("$f_time", typeFactory.createTypeWithNullability(time, type == WindowJoin.Type.LEFT || type == WindowJoin.Type.FULL))
                .uniquify()
                .build();
            RelNode windowJoin = new NesWindowJoin(
                join.getCluster(), join.getTraitSet(), left, right, spec, split.condition(), split.residual(), rowType);
            RelBuilder builder = call.builder().push(windowJoin);
            List<RexNode> projections = new ArrayList<>();
            for (int i = 0; i < join.getRowType().getFieldCount(); i++) {
                projections.add(builder.field(i < leftWidth ? 2 + i : 3 + i));
            }
            builder.project(projections, join.getRowType().getFieldNames(), true);
            call.transformTo(builder.build());
        }
    }
}

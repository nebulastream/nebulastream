package nes.calcite;

import com.google.protobuf.TextFormat;
import java.util.List;
import nes.calcite.proto.WindowJoin;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A windowed join of two streams as NebulaStream executes it. Output: window_start, window_end, left columns, right
 * columns. {@code condition} refers to [left columns..., right columns...] and is null for a pure window join. For a
 * hash join it only holds the leftField = rightField keys and {@code residual} the rest of the condition.
 */
public final class NesWindowJoin extends BiRel {
    final WindowJoin spec;
    final @Nullable RexNode condition;
    final List<RexNode> residual;

    NesWindowJoin(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode left,
        RelNode right,
        WindowJoin spec,
        @Nullable RexNode condition,
        List<RexNode> residual,
        RelDataType rowType) {
        super(cluster, traits, left, right);
        this.spec = spec;
        this.condition = condition;
        this.residual = residual;
        /// Our names: START, END, the left input's columns, the right input's (also after a rule renames an input).
        java.util.List<String> names = new java.util.ArrayList<>(NesWindowAggregate.WINDOW_COLUMNS);
        names.addAll(left.getRowType().getFieldNames());
        names.addAll(right.getRowType().getFieldNames());
        this.rowType = NesWindowAggregate.withNames(cluster.getTypeFactory(), rowType, names);
    }

    /** The plan with every window join as a nested-loop join: it checks the keys with the rest of the condition. */
    static RelNode nestedLoop(RelNode plan) {
        return plan.accept(new org.apache.calcite.rel.RelHomogeneousShuttle() {
            @Override
            public RelNode visit(RelNode other) {
                RelNode node = super.visit(other);
                if (!(node instanceof NesWindowJoin join) || join.spec.getImplementation() == WindowJoin.Implementation.NESTED_LOOP_JOIN) {
                    return node;
                }
                WindowJoin spec = join.spec.toBuilder().setImplementation(WindowJoin.Implementation.NESTED_LOOP_JOIN).build();
                return new NesWindowJoin(join.getCluster(), join.getTraitSet(), join.getLeft(), join.getRight(), spec, join.condition,
                    join.residual, join.getRowType());
            }
        });
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new NesWindowJoin(getCluster(), traitSet, inputs.get(0), inputs.get(1), spec, condition, residual, rowType);
    }

    /** Expression rewrites (e.g. Planner's CASE result casts, Unsigned) also reach the join condition and residual. */
    @Override
    public RelNode accept(RexShuttle shuttle) {
        RexNode newCondition = condition == null ? null : condition.accept(shuttle);
        List<RexNode> newResidual = shuttle.apply(residual);
        if (newCondition == condition && newResidual.equals(residual)) {
            return this;
        }
        return new NesWindowJoin(getCluster(), traitSet, left, right, spec, newCondition, newResidual, rowType);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("spec", TextFormat.shortDebugString(spec)).itemIf("condition", condition, condition != null)
            .itemIf("residual", residual, !residual.isEmpty());
    }
}

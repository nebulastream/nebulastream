package nes.calcite;

import com.google.protobuf.TextFormat;
import java.util.List;
import nes.calcite.proto.WindowAggregate;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;

/**
 * A windowed aggregation as NebulaStream executes it. Output: window_start, window_end, group keys, aggregates.
 * The window parameters are kept as the protobuf payload that is shipped in Substrait.
 */
public final class NesWindowAggregate extends SingleRel {
    final WindowAggregate spec;

    NesWindowAggregate(RelOptCluster cluster, RelTraitSet traits, RelNode input, WindowAggregate spec, RelDataType rowType) {
        super(cluster, traits, input);
        this.spec = spec;
        this.rowType = rowType;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new NesWindowAggregate(getCluster(), traitSet, sole(inputs), spec, rowType);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("spec", TextFormat.shortDebugString(spec));
    }
}

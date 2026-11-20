package nes.calcite;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

/**
 * A projection as NebulaStream executes it: named assignments {@code field := expression}, one per output column, in
 * this order. Each expression reads the input row. NesNames makes these from Calcite's projections after all rules, so
 * that Calcite's rules (which only know Project) never see it; it is shipped as a Projection payload.
 */
public final class NesProjection extends SingleRel {
    final List<RexNode> expressions;

    /// rowType: the output columns; their names are the fields the assignments write.
    NesProjection(RelOptCluster cluster, RelTraitSet traits, RelNode input, List<RexNode> expressions, RelDataType rowType) {
        super(cluster, traits, input);
        this.expressions = List.copyOf(expressions);
        this.rowType = rowType;
    }

    List<String> fields() {
        return rowType.getFieldNames();
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new NesProjection(getCluster(), traitSet, sole(inputs), expressions, rowType);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        for (int i = 0; i < expressions.size(); i++) {
            pw.item(fields().get(i), expressions.get(i));
        }
        return pw;
    }
}

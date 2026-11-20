package nes.calcite;

import com.google.protobuf.TextFormat;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import nes.calcite.proto.WindowAggregate;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;

/**
 * A windowed aggregation as NebulaStream executes it. Output: START, END, group keys, aggregates.
 * The window parameters are kept as the protobuf payload that is shipped in Substrait.
 *
 * <p>The columns have our names: START and END, each group key the name of its input column (also after a rule renames
 * the input), then the aggregates as given. So the converter adds no projection to rename them.
 */
public final class NesWindowAggregate extends SingleRel {
    final WindowAggregate spec;

    NesWindowAggregate(RelOptCluster cluster, RelTraitSet traits, RelNode input, WindowAggregate spec, RelDataType rowType) {
        super(cluster, traits, input);
        this.spec = spec;
        List<String> names = new ArrayList<>(WINDOW_COLUMNS);
        for (int key : spec.getGroupKeysList()) {
            names.add(input.getRowType().getFieldNames().get(key));
        }
        names.addAll(rowType.getFieldNames().subList(names.size(), rowType.getFieldCount()));
        this.rowType = withNames(cluster.getTypeFactory(), rowType, names);
    }

    /** Our names of the window columns. */
    static final List<String> WINDOW_COLUMNS = List.of("START", "END");

    /** The row type with these names, made unique (a later duplicate gets a suffix). */
    static RelDataType withNames(RelDataTypeFactory types, RelDataType rowType, List<String> names) {
        return types.createStructType(rowType.getFieldList().stream().map(RelDataTypeField::getType).toList(),
            SqlValidatorUtil.uniquify(names, true));
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

package nes.calcite;

import java.util.ArrayList;
import java.util.List;
import nes.calcite.proto.Field;
import nes.calcite.proto.InferModel;
import nes.calcite.proto.Model;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

/**
 * Model inference as NebulaStream executes it (InferModel): runs the model on each row of the input. The model reads its
 * inputs from the input columns with the names of its input fields; the output is the input's columns followed by the
 * model's outputs. Made from MODEL_INFERENCE by ModelInference.RULE.
 */
public final class NesModelInference extends SingleRel {
    final Model model;

    /// rowType: the input's columns, then the model's outputs.
    NesModelInference(RelOptCluster cluster, RelTraitSet traits, RelNode input, Model model, RelDataType rowType) {
        super(cluster, traits, input);
        this.model = model;
        this.rowType = rowType;
    }

    /** The input column of each model input, in the model's order: the column of that name. */
    List<Integer> inputColumns() {
        List<RelDataTypeField> columns = getInput().getRowType().getFieldList();
        List<Integer> indexes = new ArrayList<>();
        for (Field field : model.getInputsList()) {
            int index = columns.stream().filter(c -> c.getName().equals(field.getName())).mapToInt(RelDataTypeField::getIndex)
                .findFirst().orElseGet(() -> columns.stream().filter(c -> c.getName().equalsIgnoreCase(field.getName()))
                    .mapToInt(RelDataTypeField::getIndex).findFirst().orElse(-1));
            if (index < 0) {
                throw new IllegalArgumentException("model " + model.getName() + " reads the column " + field.getName()
                    + ", which its input does not have; the input's columns: " + getInput().getRowType().getFieldNames());
            }
            indexes.add(index);
        }
        return indexes;
    }

    InferModel spec() {
        List<String> names = rowType.getFieldNames();
        int inputs = getInput().getRowType().getFieldCount();
        return InferModel.newBuilder().setModel(model.getName()).addAllOutputs(names.subList(inputs, names.size())).build();
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new NesModelInference(getCluster(), traitSet, sole(inputs), model, rowType);
    }

    /// The model, the input columns it reads, and the columns it appends.
    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw).item("model", model.getName());
        List<String> inputNames = getInput().getRowType().getFieldNames();
        for (int column : inputColumns()) {
            pw.item("reads " + inputNames.get(column), "$" + column);
        }
        List<RelDataTypeField> fields = rowType.getFieldList();
        for (int i = getInput().getRowType().getFieldCount(); i < fields.size(); i++) {
            pw.item("appends " + fields.get(i).getName(), "$" + i + " " + fields.get(i).getType());
        }
        return pw;
    }
}

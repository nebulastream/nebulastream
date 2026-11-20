package nes.calcite;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

/**
 * The last step of the logical plan: the columns get the names our operators use. Calcite finds a column by its
 * position and needs no names; our operators find it by its name. So:
 * <ul>
 *   <li>each projection becomes a NesProjection, named assignments as our projection has them;</li>
 *   <li>the inputs of a union get the union's names (ours matches the inputs' columns by name);</li>
 *   <li>the inputs of a window join get unique names: our join's columns are START, END and those of both inputs, and
 *       no two may have the same name (a self-join gives the right side's ID the name ID0, as Calcite does);</li>
 *   <li>a group key of a window aggregation gets the name it has in the output (another than START or END).</li>
 * </ul>
 * A rename becomes part of a NesProjection below, else a NesProjection of its own. It runs after all rules, which could
 * otherwise remove a projection that only renames.
 */
final class NesNames {
    private NesNames() {}

    static RelNode apply(RelNode plan) {
        return plan.accept(new RelHomogeneousShuttle() {
            @Override
            public RelNode visit(RelNode other) {
                return name(super.visit(other));
            }
        });
    }

    private static RelNode name(RelNode node) {
        if (node instanceof Project project) {
            return new NesProjection(project.getCluster(), project.getTraitSet(), project.getInput(), project.getProjects(),
                project.getRowType());
        }
        if (node instanceof Union union) {
            List<String> names = union.getRowType().getFieldNames();
            return union.copy(union.getTraitSet(), union.getInputs().stream().map(input -> rename(input, names)).toList());
        }
        if (node instanceof NesWindowJoin join) {
            List<String> names = join.getRowType().getFieldNames();
            int leftWidth = join.getLeft().getRowType().getFieldCount();
            RelNode left = rename(join.getLeft(), names.subList(2, 2 + leftWidth));
            RelNode right = rename(join.getRight(), names.subList(2 + leftWidth, names.size()));
            return join.copy(join.getTraitSet(), List.of(left, right));
        }
        if (node instanceof NesWindowAggregate window) {
            List<String> names = new ArrayList<>(window.getInput().getRowType().getFieldNames());
            List<Integer> keys = window.spec.getGroupKeysList();
            for (int i = 0; i < keys.size(); i++) {
                names.set(keys.get(i), window.getRowType().getFieldNames().get(2 + i));
            }
            return window.copy(window.getTraitSet(), List.of(rename(window.getInput(), names)));
        }
        return node;
    }

    /** The relation with these column names. */
    private static RelNode rename(RelNode input, List<String> names) {
        if (input.getRowType().getFieldNames().equals(names)) {
            return input;
        }
        RelDataType renamed = NesWindowAggregate.withNames(input.getCluster().getTypeFactory(), input.getRowType(), names);
        if (input instanceof NesProjection projection) {
            return new NesProjection(projection.getCluster(), projection.getTraitSet(), projection.getInput(),
                projection.expressions, renamed);
        }
        List<RexNode> columns = new ArrayList<>();
        for (int i = 0; i < names.size(); i++) {
            columns.add(RexInputRef.of(i, input.getRowType()));
        }
        return new NesProjection(input.getCluster(), input.getTraitSet(), input, columns, renamed);
    }
}

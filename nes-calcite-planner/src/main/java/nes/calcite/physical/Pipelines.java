package nes.calcite.physical;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;

/** The last pass over a plan of NES physical operators (NesPhysical). */
public final class Pipelines {
    private Pipelines() {}

    public static RelNode strip(RelNode node) {
        return node instanceof RelSubset subset ? subset.getBest() : node;
    }

    /**
     * Resolves the helpers that are not engine operators, so that a buffer is only written and read again where a
     * pipeline must end:
     * <ul>
     *   <li>UnionAll: the operators above it are copied into the pipeline of each input, up to the pipeline's Emit. Each
     *   input writes into the next pipeline (e.g. the sink) on its own; no pipeline merges them.</li>
     *   <li>Retain: the Emit that ends the pipeline keeps the retained fields; the operators in between see the wider
     *   row, so their field references are renumbered.</li>
     * </ul>
     */
    public static RelNode finish(RelNode sink) {
        List<RelNode> inputs = new ArrayList<>();
        for (RelNode input : sink.getInputs()) {
            rewrite(input).forEach(branch -> inputs.add(branch.node()));
        }
        return sink.copy(sink.getTraitSet(), inputs);
    }

    /** One copy of an operator: the rewritten node, and where each field of the original node's row is in its row. */
    private record Branch(RelNode node, int[] fields) {
        static Branch of(RelNode node) {
            int[] fields = new int[node.getRowType().getFieldCount()];
            for (int i = 0; i < fields.length; i++) {
                fields[i] = i;
            }
            return new Branch(node, fields);
        }
    }

    private static List<Branch> rewrite(RelNode original) {
        RelNode node = strip(original);
        List<Branch> branches = new ArrayList<>();
        if (node instanceof NesPhysical.UnionAll union) {
            /// Field i of the union is field i of each input.
            union.getInputs().forEach(input -> branches.addAll(rewrite(input)));
        } else if (node instanceof NesPhysical.Retain retain) {
            for (Branch below : rewrite(retain.getInput())) {
                int[] fields = retain.fields.stream().mapToInt(field -> below.fields()[field]).toArray();
                branches.add(new Branch(below.node(), fields));
            }
        } else if (node instanceof NesOperator operator && (operator.startsPipeline() || operator.getInputs().isEmpty())) {
            /// Starts a pipeline: each copy of its inputs (one per branch below) is an input, e.g. the builds of a probe.
            List<RelNode> inputs = new ArrayList<>();
            operator.getInputs().forEach(input -> rewrite(input).forEach(branch -> inputs.add(branch.node())));
            branches.add(Branch.of(inputs.isEmpty() ? operator : operator.copy(operator.getTraitSet(), inputs)));
        } else if (node instanceof NesOperator operator) {
            /// Continues the pipeline of each branch below, over its renumbered fields.
            for (Branch below : rewrite(operator.getInput(0))) {
                NesOperator copy = operator.over((NesOperator) below.node(), below.fields());
                branches.add(new Branch(copy, operator.fields(below.fields(), copy)));
            }
        } else {
            throw new UnsupportedOperationException("not a physical operator: " + node.getRelTypeName());
        }
        return branches;
    }
}

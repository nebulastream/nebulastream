package nes.calcite.physical;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;

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
        if (node instanceof NesPhysical.SourceScan || node instanceof NesPhysical.NetworkSource) {
            branches.add(Branch.of(node));
        } else if (node instanceof NesPhysical.Scan scan) {
            List<RelNode> inputs = new ArrayList<>();
            scan.getInputs().forEach(input -> rewrite(input).forEach(branch -> inputs.add(branch.node())));
            branches.add(Branch.of(scan.copy(scan.getTraitSet(), inputs)));
        } else if (node instanceof NesPhysical.WindowProbe probe) {
            /// Starts a pipeline: each copy of its build (one per branch below) is an input.
            List<RelNode> inputs = new ArrayList<>();
            probe.getInputs().forEach(input -> rewrite(input).forEach(branch -> inputs.add(branch.node())));
            branches.add(Branch.of(probe.copy(probe.getTraitSet(), inputs)));
        } else if (node instanceof NesPhysical.WatermarkAssigner assigner) {
            for (Branch below : rewrite(assigner.getInput())) {
                branches.add(new Branch(earliest(assigner, below.node(), below.fields()[assigner.timeField]), below.fields()));
            }
        } else if (node instanceof NesPhysical.WindowBuild build) {
            /// Ends the pipeline of each branch; all copies fill the same window state.
            for (Branch below : rewrite(build.getInput())) {
                int[] fields = below.fields();
                RelNode copy = new NesPhysical.WindowBuild(build.getCluster(), build.getTraitSet(), below.node(), build.window,
                    fields[build.timeField], build.keyFields.stream().map(f -> fields[f]).toList(),
                    build.aggregationFields.stream().map(f -> f < 0 ? f : fields[f]).toList(), build.probeRowType);
                branches.add(Branch.of(copy));
            }
        } else if (node instanceof NesPhysical.JoinProbe probe) {
            /// Starts a pipeline: each copy of its builds (one per branch below) is an input.
            List<RelNode> inputs = new ArrayList<>();
            probe.getInputs().forEach(input -> rewrite(input).forEach(branch -> inputs.add(branch.node())));
            branches.add(Branch.of(probe.copy(probe.getTraitSet(), inputs)));
        } else if (node instanceof NesPhysical.JoinBuild build) {
            /// Ends the pipeline of each branch; all copies of a side store into the same state.
            for (Branch below : rewrite(build.getInput())) {
                int[] fields = below.fields();
                branches.add(Branch.of(new NesPhysical.JoinBuild(build.getCluster(), build.getTraitSet(), below.node(), build.join,
                    build.right, fields[build.timeField], build.storedFields.stream().map(f -> fields[f]).toList())));
            }
        } else if (node instanceof NesPhysical.UnionAll union) {
            /// Field i of the union is field i of each input.
            union.getInputs().forEach(input -> branches.addAll(rewrite(input)));
        } else if (node instanceof NesPhysical.Retain retain) {
            for (Branch below : rewrite(retain.getInput())) {
                int[] fields = retain.fields.stream().mapToInt(field -> below.fields()[field]).toArray();
                branches.add(new Branch(below.node(), fields));
            }
        } else if (node instanceof NesPhysical.Selection selection) {
            for (Branch below : rewrite(selection.getInput())) {
                RelNode copy = new NesPhysical.Selection(
                    selection.getCluster(), selection.getTraitSet(), below.node(), renumber(selection.condition, below.fields()));
                branches.add(new Branch(copy, below.fields()));
            }
        } else if (node instanceof NesPhysical.Map map) {
            for (Branch below : rewrite(map.getInput())) {
                RelNode copy = new NesPhysical.Map(map.getCluster(), map.getTraitSet(), below.node(), map.fields,
                    map.expressions.stream().map(expression -> renumber(expression, below.fields())).toList());
                int width = copy.getRowType().getFieldCount();
                int[] fields = new int[below.fields().length + map.fields.size()];
                System.arraycopy(below.fields(), 0, fields, 0, below.fields().length);
                for (int i = 0; i < map.fields.size(); i++) {
                    fields[below.fields().length + i] = width - map.fields.size() + i;
                }
                branches.add(new Branch(copy, fields));
            }
        } else if (node instanceof NesPhysical.InferModel infer) {
            /// As a Map: reads fields of its input row, appends its outputs.
            for (Branch below : rewrite(infer.getInput())) {
                RelNode copy = new NesPhysical.InferModel(infer.getCluster(), infer.getTraitSet(), below.node(), infer.model,
                    infer.inputFields.stream().map(field -> below.fields()[field]).toList(), infer.outputFields, infer.outputTypes);
                int width = copy.getRowType().getFieldCount();
                int[] fields = new int[below.fields().length + infer.outputFields.size()];
                System.arraycopy(below.fields(), 0, fields, 0, below.fields().length);
                for (int i = 0; i < infer.outputFields.size(); i++) {
                    fields[below.fields().length + i] = width - infer.outputFields.size() + i;
                }
                branches.add(new Branch(copy, fields));
            }
        } else if (node instanceof NesPhysical.Emit emit) {
            for (Branch below : rewrite(emit.getInput())) {
                List<Integer> fields = emit.fields.stream().map(field -> below.fields()[field]).toList();
                branches.add(Branch.of(new NesPhysical.Emit(emit.getCluster(), emit.getTraitSet(), below.node(), fields)));
            }
        } else {
            throw new UnsupportedOperationException("not a physical operator: " + node.getRelTypeName());
        }
        return branches;
    }

    /**
     * Puts the watermark assigner as early in the pipeline as the time field allows: below Selections (so that the
     * watermark also advances with rows that are filtered out) and below Maps that do not compute the time field.
     */
    private static RelNode earliest(NesPhysical.WatermarkAssigner assigner, RelNode node, int timeField) {
        boolean below = (node instanceof NesPhysical.Selection || node instanceof NesPhysical.Map)
            && timeField < node.getInput(0).getRowType().getFieldCount();
        if (below) {
            return node.copy(node.getTraitSet(), List.of(earliest(assigner, node.getInput(0), timeField)));
        }
        return new NesPhysical.WatermarkAssigner(assigner.getCluster(), assigner.getTraitSet(), node, timeField);
    }

    private static RexNode renumber(RexNode expression, int[] fields) {
        return expression.accept(new RexShuttle() {
            @Override
            public RexNode visitInputRef(RexInputRef ref) {
                return new RexInputRef(fields[ref.getIndex()], ref.getType());
            }
        });
    }
}

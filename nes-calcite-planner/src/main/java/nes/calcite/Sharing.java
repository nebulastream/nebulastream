package nes.calcite;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.util.Pair;

/**
 * Makes identical subtrees of a plan one shared node, as HepPlanner (the rule steps) and VolcanoPlanner do: two operators
 * are identical if they have the same kind, attributes, row type and (shared) inputs. Steps that rebuild the plan node by
 * node (e.g. RelDecorrelator, RelFieldTrimmer) copy a shared node once per use; after them the plan is shared again, so
 * that every step's plan shares what the final plan shares. Identical subtrees compute the same rows, so sharing them
 * does not change the result.
 */
final class Sharing {
    private Sharing() {}

    static RelNode share(RelNode plan) {
        return new Sharing.Pass().visit(plan);
    }

    private static final class Pass {
        /// Each visited node's shared node, and the shared nodes by their key.
        private final Map<RelNode, RelNode> visited = new IdentityHashMap<>();
        private final Map<String, RelNode> shared = new HashMap<>();
        private final Map<RelNode, Integer> ids = new IdentityHashMap<>();

        RelNode visit(RelNode node) {
            RelNode done = visited.get(node);
            if (done != null) {
                return done;
            }
            List<RelNode> inputs = new ArrayList<>();
            boolean changed = false;
            for (RelNode input : node.getInputs()) {
                RelNode sharedInput = visit(input);
                inputs.add(sharedInput);
                changed |= sharedInput != input;
            }
            RelNode copy = changed ? node.copy(node.getTraitSet(), inputs) : node;
            RelNode result = shared.computeIfAbsent(key(copy, inputs), key -> copy);
            ids.putIfAbsent(result, ids.size());
            visited.put(node, result);
            return result;
        }

        /** The operator's kind, attributes (as its explain output has them, without its inputs), row type and inputs. */
        private String key(RelNode node, List<RelNode> inputs) {
            StringBuilder key = new StringBuilder(node.getRelTypeName()).append(node.getTraitSet());
            node.explain(new RelWriterImpl(new java.io.PrintWriter(java.io.Writer.nullWriter())) {
                @Override
                protected void explain_(RelNode rel, List<Pair<String, Object>> values) {
                    for (Pair<String, Object> term : values) {
                        if (!(term.right instanceof RelNode)) {
                            key.append('|').append(term.left).append('=').append(term.right);
                        }
                    }
                }
            });
            key.append('|').append(node.getRowType().getFullTypeString());
            for (RelNode input : inputs) {
                key.append("|input#").append(ids.get(input));
            }
            return key.toString();
        }
    }
}

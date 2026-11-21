package nes.calcite.rules;

import nes.calcite.logical.AnonymousSources;
import nes.calcite.logical.NesLogical;
import nes.calcite.logical.NesWindowJoin;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rex.RexInputRef;

/**
 * Puts the watermark assigner of each window input directly after its sources, before placement. So the assigner is the
 * first operator after the source (on the source's worker), and every row of the source, also one that a later filter
 * drops, advances the watermark. As our own optimizer places it.
 */
public final class Watermarks {
    private Watermarks() {}

    public static RelNode assign(RelNode plan) {
        Map<RelNode, Integer> timeFields = new IdentityHashMap<>();
        collect(plan, timeFields);
        if (timeFields.isEmpty()) {
            return plan;
        }
        return plan.accept(new RelHomogeneousShuttle() {
            @Override
            public RelNode visit(RelNode other) {
                Integer timeField = timeFields.get(other);
                RelNode node = super.visit(other);
                return timeField == null ? node : new NesLogical.WatermarkAssigner(node.getCluster(), node.getTraitSet(), node, timeField);
            }
        });
    }

    /** For each window input, the sources and the column of each that its time field passes on. */
    private static void collect(RelNode node, Map<RelNode, Integer> timeFields) {
        node.getInputs().forEach(input -> collect(input, timeFields));
        if (node instanceof NesLogical.WindowAggregation aggregate) {
            sources(aggregate.getInput(), aggregate.spec.getTimeField(), timeFields);
        } else if (node instanceof NesWindowJoin join) {
            sources(join.getLeft(), join.spec.getLeftTimeField(), timeFields);
            sources(join.getRight(), join.spec.getRightTimeField(), timeFields);
        }
    }

    /**
     * Adds the sources whose column the field passes on, unchanged, through filters, projections and unions. Rows from another
     * window operator have its watermarks. If the field is computed (e.g. the constant time of a GLOBAL window), or a source
     * is assigned another time field already, nothing is added: the window gets its assigner directly before it
     * (NesPhysical.watermarked).
     */
    private static void sources(RelNode input, int field, Map<RelNode, Integer> timeFields) {
        Map<RelNode, Integer> found = new IdentityHashMap<>();
        if (trace(input, field, found) && found.entrySet().stream()
            .allMatch(e -> timeFields.getOrDefault(e.getKey(), e.getValue()).equals(e.getValue()))) {
            timeFields.putAll(found);
        }
    }

    private static boolean trace(RelNode rel, int field, Map<RelNode, Integer> found) {
        if (rel instanceof NesLogical.PhysicalSourceScan || rel instanceof AnonymousSources.NesAnonymousSource) {
            found.put(rel, field);
            return true;
        }
        if (rel instanceof NesLogical.WindowAggregation || rel instanceof NesWindowJoin) {
            return true;
        }
        if (rel instanceof Filter filter) {
            return trace(filter.getInput(), field, found);
        }
        if (rel instanceof Project project) {
            return project.getProjects().get(field) instanceof RexInputRef ref && trace(project.getInput(), ref.getIndex(), found);
        }
        if (rel instanceof Union union) {
            List<RelNode> inputs = union.getInputs();
            return inputs.stream().allMatch(input -> trace(input, field, found));
        }
        return false;
    }
}

package nes.calcite.physical;

import nes.calcite.logical.AnonymousSources;
import nes.calcite.logical.NesLogical;
import nes.calcite.logical.NesModelInference;
import nes.calcite.logical.NesWindowJoin;
import nes.calcite.steps.PlanGraph;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import nes.calcite.proto.RuleApplication;
import nes.calcite.proto.WindowJoin;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptListener;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

/**
 * Which converter rule turned each logical operator into which physical operators, and why: for the planner's steps.
 * Calcite's VolcanoPlanner reports each rule firing; our rules (NesPhysical.RULES) convert each logical operator in one
 * way, so the reason is what the rule produced (e.g. a hash join for keys), not a choice between costed alternatives.
 */
public final class Conversions implements RelOptListener {
    private record Conversion(org.apache.calcite.plan.RelOptRule rule, RelNode physical) {}

    private final VolcanoPlanner volcano;
    /// Registered logical operator -> its first conversion.
    private final Map<RelNode, Conversion> conversions = new IdentityHashMap<>();

    public Conversions(VolcanoPlanner volcano) {
        this.volcano = volcano;
    }

    @Override
    public void ruleProductionSucceeded(RuleProductionEvent event) {
        RelNode logical = event.getRuleCall().rels[0];
        if (!event.isBefore() && !(logical instanceof NesLogical.Parts)) {
            conversions.putIfAbsent(logical, new Conversion(event.getRuleCall().getRule(), event.getRel()));
        }
    }

    @Override
    public void relEquivalenceFound(RelEquivalenceEvent event) {}

    @Override
    public void ruleAttempted(RuleAttemptedEvent event) {}

    @Override
    public void relDiscarded(RelDiscardedEvent event) {}

    @Override
    public void relChosen(RelChosenEvent event) {}

    /**
     * The conversions one by one, from the sources up: after each, the parts (one per planned part, in order) with the
     * operators converted so far, the latest highlighted.
     */
    public List<RuleApplication> replay(List<String> workers, List<PlanGraph.Output> outputs) {
        RelNode holder = logical((RelSubset) volcano.getRoot());
        List<RelNode> order = new ArrayList<>();
        holder.getInputs().forEach(part -> postorder(logical(part), order));
        List<RuleApplication> applications = new ArrayList<>();
        Set<RelNode> converted = java.util.Collections.newSetFromMap(new IdentityHashMap<>());
        for (RelNode logical : order) {
            converted.add(logical);
            Set<RelNode> latest = java.util.Collections.newSetFromMap(new IdentityHashMap<>());
            List<PlanGraph.Part> parts = new ArrayList<>();
            StringBuilder text = new StringBuilder();
            for (int i = 0; i < holder.getInputs().size(); i++) {
                RelNode plan = hybrid(logical(holder.getInput(i)), converted, logical, latest);
                parts.add(new PlanGraph.Part(workers.get(i), plan, outputs.get(i)));
                text.append(workers.get(i) == null ? "" : "on " + workers.get(i) + ":\n").append(RelOptUtil.toString(plan));
            }
            Conversion conversion = conversions.get(logical);
            applications.add(RuleApplication.newBuilder()
                .setRule(conversion.rule() + ": " + reason(conversion.rule(), logical, conversion.physical()))
                .setOurs(true)
                .setPlan(text.toString())
                .setDot(PlanGraph.of(parts, latest::contains, false))
                .build());
        }
        return applications;
    }

    private void postorder(RelNode logical, List<RelNode> order) {
        logical.getInputs().forEach(input -> postorder(logical(input), order));
        if (conversions.containsKey(logical)) {
            order.add(logical);
        }
    }

    /** The plan of the logical operator with the converted ones replaced by their physical operators. */
    private RelNode hybrid(RelNode logical, Set<RelNode> converted, RelNode latest, Set<RelNode> latestNodes) {
        if (converted.contains(logical)) {
            return copy(conversions.get(logical).physical(), converted, latest, logical == latest ? latestNodes : null);
        }
        if (logical.getInputs().isEmpty()) {
            return logical;
        }
        /// A logical operator that reads its input's columns by name (NesModelInference) does not take physical operators,
        /// whose fields may have fresh names: it is shown over its logical inputs until it is converted.
        boolean byName = logical instanceof NesModelInference;
        List<RelNode> inputs = logical.getInputs().stream()
            .map(input -> hybrid(logical(input), byName ? java.util.Collections.emptySet() : converted, latest, byName ? null : latestNodes))
            .toList();
        return logical.copy(logical.getTraitSet(), inputs);
    }

    /** A copy of a rule's physical operators, whose inputs (subsets) are the plans of the logical inputs. */
    private RelNode copy(RelNode node, Set<RelNode> converted, RelNode latest, Set<RelNode> collect) {
        if (node instanceof RelSubset subset) {
            return hybrid(logical(subset), converted, latest, null);
        }
        List<RelNode> inputs = node.getInputs().stream().map(input -> copy(input, converted, latest, collect)).toList();
        RelNode copy = inputs.isEmpty() ? node : node.copy(node.getTraitSet(), inputs);
        if (collect != null) {
            collect.add(copy);
        }
        return copy;
    }

    /** The logical operator of a subset's set (as registered: its inputs are subsets). */
    private RelNode logical(RelNode node) {
        if (!(node instanceof RelSubset subset)) {
            return node;
        }
        RelNode any = subset.getRelList().isEmpty() ? subset.getOriginal() : subset.getRelList().get(0);
        RelSubset none = volcano.getSubset(any, any.getTraitSet().replace(Convention.NONE));
        for (RelNode rel : none == null ? List.<RelNode>of() : none.getRelList()) {
            if (!(rel instanceof NesPhysical.NesRel) && !(rel instanceof RelSubset)) {
                return rel;
            }
        }
        return any;
    }

    /** Why the rule produced these physical operators (its ExplainedRule.reason). */
    static String reason(org.apache.calcite.plan.RelOptRule rule, RelNode logical, RelNode physical) {
        List<RelNode> produced = new ArrayList<>();
        collectProduced(physical, produced);
        return rule instanceof ExplainedRule explained ? explained.reason(logical, produced) : "converted by our rule.";
    }

    /** The physical operators that the rule created (not those of its inputs). */
    private static void collectProduced(RelNode node, List<RelNode> produced) {
        if (node instanceof RelSubset) {
            return;
        }
        produced.add(node);
        node.getInputs().forEach(input -> collectProduced(input, produced));
    }
}

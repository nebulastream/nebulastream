package nes.calcite;

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
final class Conversions implements RelOptListener {
    private record Conversion(String rule, RelNode physical) {}

    private final VolcanoPlanner volcano;
    /// Registered logical operator -> its first conversion.
    private final Map<RelNode, Conversion> conversions = new IdentityHashMap<>();

    Conversions(VolcanoPlanner volcano) {
        this.volcano = volcano;
    }

    @Override
    public void ruleProductionSucceeded(RuleProductionEvent event) {
        RelNode logical = event.getRuleCall().rels[0];
        if (!event.isBefore() && !(logical instanceof NesLogical.Parts)) {
            conversions.putIfAbsent(logical, new Conversion(event.getRuleCall().getRule().toString(), event.getRel()));
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
    List<RuleApplication> replay(List<String> workers, List<PlanGraph.Output> outputs) {
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
                .setRule(conversion.rule() + ": " + reason(logical, conversion.physical()))
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

    /** Why the rule produced these physical operators. */
    static String reason(RelNode logical, RelNode physical) {
        List<RelNode> produced = new ArrayList<>();
        collectProduced(physical, produced);
        boolean assigner = produced.stream().anyMatch(n -> n instanceof NesPhysical.WatermarkAssigner);
        String watermarks = assigner
            ? " An assigner before it sets the watermarks: the time field is computed, not a source column."
            : " Its rows have watermarks already: from the assigners after the sources, another window, or the network.";
        if (logical instanceof NesLogical.PhysicalSourceScan scan) {
            return "reads physical source " + scan.physicalSource.getId() + " on " + scan.physicalSource.getHost()
                + ": a SourceScan, and a Scan that parses its rows.";
        }
        if (logical instanceof AnonymousSources.NesAnonymousSource source) {
            return "reads the anonymous " + source.spec.getType() + " source on " + source.host() + ": a SourceScan, and a Scan that parses its rows.";
        }
        if (logical instanceof NesLogical.NetworkSource network) {
            return "receives the rows of " + network.upstreamHost + ": a NetworkSource, and a Scan that reads their native format.";
        }
        if (logical instanceof NesLogical.WatermarkAssigner assigner1) {
            return "the first operator after the source sets the watermarks from "
                + assigner1.getRowType().getFieldNames().get(assigner1.timeField) + ".";
        }
        if (logical instanceof Filter) {
            return "a Selection with the filter's condition.";
        }
        if (logical instanceof Project project) {
            List<String> computed = new ArrayList<>();
            List<String> passed = new ArrayList<>();
            for (int i = 0; i < project.getProjects().size(); i++) {
                (project.getProjects().get(i) instanceof RexInputRef ? passed : computed).add(project.getRowType().getFieldNames().get(i));
            }
            return (computed.isEmpty() ? "no field is computed" : "a Map computes " + computed)
                + (passed.isEmpty() ? "" : "; " + passed + " are input fields") + "; a Retain keeps the projected fields"
                + " (the Emit that ends the pipeline writes only them).";
        }
        if (logical instanceof Union) {
            return "a UnionAll: each input runs the operators above it in its own pipeline (Pipelines.finish); no operator merges"
                + " them." + (produced.stream().anyMatch(n -> n instanceof NesPhysical.Map)
                    ? " A Map casts the fields of an input whose types differ from the union's." : "");
        }
        if (logical instanceof NesModelInference inference) {
            return "an InferModel that reads " + inference.inputColumns() + " and appends the model's outputs.";
        }
        if (logical instanceof NesWindowAggregate aggregate) {
            return "a " + aggregate.spec.getKind() + " window aggregation: a WindowBuild ends the input's pipeline and fills the"
                + " window state, a WindowProbe starts a pipeline and emits the closed windows." + watermarks;
        }
        if (logical instanceof NesWindowJoin join) {
            String implementation = join.spec.getImplementation() == WindowJoin.Implementation.HASH_JOIN
                ? "HASH_JOIN, as the condition has keys: " + keys(join)
                : keys(join).isEmpty() ? "NESTED_LOOP_JOIN, as the condition has no key (no left field = right field)"
                : "NESTED_LOOP_JOIN, as requested (join_strategy NESTED_LOOP_JOIN), although the condition has keys: " + keys(join);
            boolean casts = join.spec.getImplementation() == WindowJoin.Implementation.HASH_JOIN
                && produced.stream().anyMatch(n -> n instanceof NesPhysical.Map);
            return "a window join of type " + join.spec.getType() + " as " + implementation + ". A JoinBuild per side ends its pipeline and stores the"
                + " rows, a JoinProbe starts a pipeline and joins the closed windows."
                + (casts ? " A Map casts keys of different types to one type, as the hash table needs equal keys." : "")
                + watermarks;
        }
        return "converted by our rule.";
    }

    /** The physical operators that the rule created (not those of its inputs). */
    private static void collectProduced(RelNode node, List<RelNode> produced) {
        if (node instanceof RelSubset) {
            return;
        }
        produced.add(node);
        node.getInputs().forEach(input -> collectProduced(input, produced));
    }

    /** The leftField = rightField conjuncts of the join's condition. */
    private static List<String> keys(NesWindowJoin join) {
        int leftWidth = join.getLeft().getRowType().getFieldCount();
        List<String> names = join.getRowType().getFieldNames();
        List<String> keys = new ArrayList<>();
        for (RexNode conjunct : RelOptUtil.conjunctions(join.condition)) {
            if (conjunct.getKind() == SqlKind.EQUALS && conjunct instanceof RexCall call
                && call.getOperands().get(0) instanceof RexInputRef a && call.getOperands().get(1) instanceof RexInputRef b
                && (a.getIndex() < leftWidth) != (b.getIndex() < leftWidth)) {
                keys.add(names.get(2 + a.getIndex()) + " = " + names.get(2 + b.getIndex()));
            }
        }
        return keys;
    }
}

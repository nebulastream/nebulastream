package nes.calcite.steps;

import nes.calcite.sql.InterpretingRexExecutor;
import java.util.ArrayList;
import java.util.List;
import nes.calcite.proto.PlanStep;
import nes.calcite.proto.RuleApplication;
import org.apache.calcite.plan.RelOptListener;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelNode;

/**
 * The plan after each step of the planner, for inspection (PlanResponse.steps; on stderr with -Dnes.calcite.trace=true).
 * detailed: also a graph per step, and the plan after each rule of the rule-based steps.
 */
public final class Steps {
    private static final boolean TRACE = Boolean.getBoolean("nes.calcite.trace");

    private final boolean detailed;
    private final List<PlanStep> steps = new ArrayList<>();
    /// The plan after the last step, to mark what a step changed (see changes).
    private RelNode previous;

    public Steps(boolean detailed) {
        this.detailed = detailed;
    }

    public List<PlanStep> list() {
        return steps;
    }

    /** A step that rebuilds the plan (no rules): its result shares identical subtrees again (see Sharing). */
    public RelNode add(String name, String origin, RelNode plan) {
        RelNode sharedPlan = Sharing.share(plan);
        add(name, origin, RelOptUtil.toString(sharedPlan), detailed ? PlanGraph.of(sharedPlan, changes()) : "", List.of());
        previous = sharedPlan;
        return sharedPlan;
    }

    public boolean detailed() {
        return detailed;
    }

    /** A step whose result is in parts, e.g. one per worker. */
    public void add(String name, String origin, List<PlanGraph.Part> parts, boolean pipelines) {
        add(name, origin, parts, pipelines, List.of());
    }

    /** A step whose result is in parts, with the plan after each rule (see rules). */
    public void add(String name, String origin, List<PlanGraph.Part> parts, boolean pipelines, List<RuleApplication> rules) {
        StringBuilder text = new StringBuilder();
        for (PlanGraph.Part part : parts) {
            text.append(part.worker() == null ? "" : "on " + part.worker() + (part.output() == null ? "" : part.output().channel() == null
                ? ", into the sink" : ", to " + part.output().downstreamHost()) + ":\n").append(RelOptUtil.toString(part.plan()));
        }
        add(name, origin, text.toString(), detailed ? PlanGraph.of(parts, node -> false, pipelines) : "", rules);
    }

    /** Runs the rules of the program; with detailed, records the plan after each rule that changes it. */
    public RelNode rules(String name, String origin, HepProgram program, RelNode plan) {
        HepPlanner planner = new HepPlanner(program);
        /// Rules fold constants with their planner's executor, and without one with Calcite's compiling RexUtil.EXECUTOR.
        planner.setExecutor(InterpretingRexExecutor.INSTANCE);
        List<RuleApplication> applied = new ArrayList<>();
        if (detailed) {
            planner.addListener(new RelOptListener() {
                @Override
                public void ruleProductionSucceeded(RuleProductionEvent event) {
                    if (event.isBefore()) {
                        return;
                    }
                    RelNode now = materialize(planner.getRoot());
                    /// HepPlanner keeps a copy of the produced operator: it is found by its text (with its inputs).
                    String produced = RelOptUtil.toString(materialize(event.getRel()));
                    org.apache.calcite.plan.RelOptRule rule = event.getRuleCall().getRule();
                    applied.add(RuleApplication.newBuilder().setRule(rule.toString())
                        .setOurs(rule.getClass().getName().startsWith("nes.calcite."))
                        .setPlan(RelOptUtil.toString(now))
                        .setDot(PlanGraph.of(now, node -> RelOptUtil.toString(node).equals(produced))).build());
                }

                @Override
                public void relEquivalenceFound(RelEquivalenceEvent event) {}

                @Override
                public void ruleAttempted(RuleAttemptedEvent event) {}

                @Override
                public void relDiscarded(RelDiscardedEvent event) {}

                @Override
                public void relChosen(RelChosenEvent event) {}
            });
        }
        planner.setRoot(plan);
        RelNode result = planner.findBestExp();
        add(name, origin, RelOptUtil.toString(result), detailed ? PlanGraph.of(result, changes()) : "", applied);
        previous = result;
        return result;
    }

    /**
     * The operators a step changed: those whose signature (the operator and its direct inputs, see Sharing.signature) the
     * plan before the step lacks. A removed operator is no longer there; the operator that took its place is marked.
     */
    private java.util.function.Predicate<RelNode> changes() {
        if (previous == null) {
            return node -> false;
        }
        java.util.Set<String> before = Sharing.signatures(previous);
        return node -> !before.contains(Sharing.signature(node));
    }

    private void add(String name, String origin, String plan, String dot, List<RuleApplication> rules) {
        steps.add(PlanStep.newBuilder().setName(name).setOrigin(origin).setPlan(plan).setDot(dot).addAllRules(rules).build());
        if (TRACE) {
            System.err.println("== " + name + "\n" + plan);
            rules.forEach(rule -> System.err.println("  -- " + rule.getRule() + "\n" + rule.getPlan()));
        }
    }

    /** A copy of HepPlanner's current plan (or a part of it) without its vertices. */
    private static RelNode materialize(RelNode node) {
        RelNode current = PlanGraph.unwrap(node);
        List<RelNode> inputs = current.getInputs().stream().map(Steps::materialize).toList();
        return inputs.isEmpty() ? current : current.copy(current.getTraitSet(), inputs);
    }
}

package nes.calcite.rules;

import org.apache.calcite.plan.RelRule;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.ImmutableBitSet;

/**
 * Conditions in OR branches that the other rules cannot use (as Hive's HivePreFilteringRule), for a filter above an inner
 * join (e.g. TPC-H Q19, whose join key and filters are repeated in each of three OR branches):
 * <ul>
 *   <li>A condition of every branch becomes a condition of its own: (A AND B) OR (A AND C) is A AND (B OR C). A join key
 *   among them makes a hash join possible.</li>
 *   <li>If every branch has conditions on one input only, their OR is implied, and added (the full OR stays): filters push
 *   it below the join, which then gets fewer rows.</li>
 * </ul>
 * Neither changes the result.
 */
public final class OrFactors {
    private OrFactors() {}

    public static final RelOptRule RULE = RuleConfig.of("NesOrFactorsRule",
        b -> b.operand(Filter.class).oneInput(b1 -> b1.operand(Join.class).anyInputs()), Rule::new).toRule();

    private static final class Rule extends RelRule<RuleConfig> {
        Rule(RuleConfig config) {
            super(config);
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            Filter filter = call.rel(0);
            Join join = call.rel(1);
            if (join.getJoinType() != JoinRelType.INNER) {
                return;
            }
            RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
            Set<RexNode> conjuncts = new LinkedHashSet<>(RelOptUtil.conjunctions(RexUtil.pullFactors(rexBuilder, filter.getCondition())));
            int left = join.getLeft().getRowType().getFieldCount();
            int width = join.getRowType().getFieldCount();
            for (RexNode conjunct : List.copyOf(conjuncts)) {
                List<RexNode> branches = RelOptUtil.disjunctions(conjunct);
                if (branches.size() < 2) {
                    continue;
                }
                for (ImmutableBitSet side : List.of(ImmutableBitSet.range(0, left), ImmutableBitSet.range(left, width))) {
                    RexNode implied = implied(rexBuilder, branches, side);
                    if (implied != null) {
                        conjuncts.add(implied);
                    }
                }
            }
            RexNode condition = RexUtil.composeConjunction(rexBuilder, conjuncts);
            if (condition.equals(filter.getCondition())
                || RelOptUtil.conjunctions(filter.getCondition()).containsAll(conjuncts)) {
                return;
            }
            call.transformTo(filter.copy(filter.getTraitSet(), join, condition));
        }
    }

    /** The OR over the branches of each branch's conditions on the fields of side, or null if a branch has none. */
    private static RexNode implied(RexBuilder rexBuilder, List<RexNode> branches, ImmutableBitSet side) {
        List<RexNode> parts = new ArrayList<>();
        for (RexNode branch : branches) {
            List<RexNode> own = RelOptUtil.conjunctions(branch).stream()
                .filter(condition -> side.contains(RelOptUtil.InputFinder.bits(condition)))
                .toList();
            if (own.isEmpty()) {
                return null;
            }
            parts.add(RexUtil.composeConjunction(rexBuilder, own));
        }
        return RexUtil.composeDisjunction(rexBuilder, parts);
    }
}

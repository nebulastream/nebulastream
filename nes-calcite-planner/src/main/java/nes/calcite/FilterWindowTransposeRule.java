package nes.calcite;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.tools.RelBuilder;

/**
 * Pushes the conjuncts of a Filter above a {@link NesWindowAggregate} that only use group keys below the window.
 * Conjuncts on window_start/window_end or on aggregates stay above.
 */
final class FilterWindowTransposeRule extends RelOptRule {
    static final FilterWindowTransposeRule INSTANCE = new FilterWindowTransposeRule();

    @SuppressWarnings("deprecation")
    private FilterWindowTransposeRule() {
        super(operand(Filter.class, operand(NesWindowAggregate.class, any())), "NesFilterWindowTransposeRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Filter filter = call.rel(0);
        NesWindowAggregate window = call.rel(1);
        List<Integer> keys = window.spec.getGroupKeysList(); // window output column 2 + i is input column keys[i]

        List<RexNode> pushed = new ArrayList<>();
        List<RexNode> kept = new ArrayList<>();
        for (RexNode conjunct : RelOptUtil.conjunctions(filter.getCondition())) {
            boolean onKeysOnly = RelOptUtil.InputFinder.bits(conjunct).asList().stream()
                .allMatch(i -> i >= 2 && i < 2 + keys.size());
            (onKeysOnly ? pushed : kept).add(conjunct);
        }
        if (pushed.isEmpty()) {
            return;
        }

        RelBuilder builder = call.builder();
        builder.push(window.getInput());
        RexShuttle toInput = new RexShuttle() {
            @Override
            public RexNode visitInputRef(RexInputRef ref) {
                return builder.field(keys.get(ref.getIndex() - 2));
            }
        };
        builder.filter(pushed.stream().map(conjunct -> conjunct.accept(toInput)).toList());
        builder.push(window.copy(window.getTraitSet(), List.of(builder.build())));
        builder.filter(kept);
        call.transformTo(builder.build());
    }
}

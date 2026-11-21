package nes.calcite.rules;

import nes.calcite.logical.NesWindowAggregate;
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
 * Pushes the conjuncts of a Filter above a {@link NesWindowAggregate} that only use group keys below the window (below
 * its window table function, which Calcite's FilterAggregateTransposeRule and FilterTableFunctionTransposeRule cannot do:
 * the window function adds columns). Conjuncts on window_start/window_end or on aggregates stay above.
 */
public final class FilterWindowTransposeRule extends RelOptRule {
    public static final FilterWindowTransposeRule INSTANCE = new FilterWindowTransposeRule();

    @SuppressWarnings("deprecation")
    private FilterWindowTransposeRule() {
        super(operand(Filter.class, operand(NesWindowAggregate.class, any())), "NesFilterWindowTransposeRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Filter filter = call.rel(0);
        NesWindowAggregate window = call.rel(1);
        List<Integer> keys = window.keys(); // output column i is column keys[i] of the window's rows

        List<RexNode> pushed = new ArrayList<>();
        List<RexNode> kept = new ArrayList<>();
        for (RexNode conjunct : RelOptUtil.conjunctions(filter.getCondition())) {
            boolean onKeysOnly = RelOptUtil.InputFinder.bits(conjunct).asList().stream().allMatch(i -> i < keys.size());
            (onKeysOnly ? pushed : kept).add(conjunct);
        }
        if (pushed.isEmpty()) {
            return;
        }

        RelBuilder builder = call.builder();
        builder.push(window.rows());
        RexShuttle toRows = new RexShuttle() {
            @Override
            public RexNode visitInputRef(RexInputRef ref) {
                return builder.field(keys.get(ref.getIndex()));
            }
        };
        builder.filter(pushed.stream().map(conjunct -> conjunct.accept(toRows)).toList());
        builder.push(window.withRows(builder.build()));
        builder.filter(kept);
        call.transformTo(builder.build());
    }
}

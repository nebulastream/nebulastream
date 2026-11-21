package nes.calcite.rules;

import org.apache.calcite.plan.RelRule;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.tools.RelBuilder;

/**
 * Moves the computed expressions of a Project above TUMBLE/HOP below it, when they do not use the window columns: e.g.
 * the CAST(i8 AS REAL) that Calcite computes for a join key of another type. The window rules (WindowAggregateRule,
 * WindowJoinRule) expect the window function directly below the join or aggregation, or below plain field references.
 */
public final class ProjectWindowTransposeRule extends RelRule<RuleConfig> {
    public static final RelOptRule INSTANCE = RuleConfig.of("NesProjectWindowTransposeRule",
        b -> b.operand(Project.class).oneInput(b1 -> b1.operand(TableFunctionScan.class).anyInputs()), ProjectWindowTransposeRule::new).toRule();

    private ProjectWindowTransposeRule(RuleConfig config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Project project = call.rel(0);
        TableFunctionScan window = call.rel(1);
        if (!(window.getCall() instanceof RexCall invocation) || window.getInputs().size() != 1
            || !List.of("TUMBLE", "HOP").contains(invocation.getOperator().getName().toUpperCase(java.util.Locale.ROOT))) {
            return;
        }
        RelNode input = window.getInputs().get(0);
        int width = input.getRowType().getFieldCount(); // window_start = width, window_end = width + 1
        List<RexNode> computed = new ArrayList<>();
        for (RexNode expression : project.getProjects()) {
            if (!(expression instanceof RexInputRef)) {
                if (RelOptUtil.InputFinder.bits(expression).asList().stream().anyMatch(i -> i >= width)) {
                    return; // uses window_start/window_end: must stay above
                }
                computed.add(expression);
            }
        }
        if (computed.isEmpty()) {
            return;
        }

        /// Below the window: the input's columns, then the computed expressions.
        RelBuilder builder = call.builder();
        builder.push(input);
        List<RexNode> below = new ArrayList<>(builder.fields());
        below.addAll(computed);
        builder.project(below);
        RelNode lower = builder.build();

        RelDataTypeFactory.Builder rowType = window.getCluster().getTypeFactory().builder().addAll(lower.getRowType().getFieldList());
        List<RelDataTypeField> windowFields = window.getRowType().getFieldList();
        rowType.add(windowFields.get(width)).add(windowFields.get(width + 1));
        RelDataType newRowType = rowType.uniquify().build();
        RelNode moved = window.copy(window.getTraitSet(), List.of(lower), window.getCall(), window.getElementType(), newRowType,
            window.getColumnMappings());

        /// Above: references into the moved window's row.
        int added = computed.size();
        RexShuttle renumber = new RexShuttle() {
            @Override
            public RexNode visitInputRef(RexInputRef ref) {
                return new RexInputRef(ref.getIndex() < width ? ref.getIndex() : ref.getIndex() + added, ref.getType());
            }
        };
        List<RexNode> above = new ArrayList<>();
        int next = width;
        for (RexNode expression : project.getProjects()) {
            above.add(expression instanceof RexInputRef ? expression.accept(renumber)
                : new RexInputRef(next++, expression.getType()));
        }
        builder.push(moved);
        builder.project(above, project.getRowType().getFieldNames(), true);
        call.transformTo(builder.build());
    }
}

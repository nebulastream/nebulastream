package nes.calcite.rules;

import java.util.function.Function;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.tools.RelBuilderFactory;

/**
 * The configuration of one of our rules (RelRule): its description, its operands and how to create the rule. Calcite
 * generates the configurations of its own rules with the Immutables annotation processor; this record is the same
 * without it.
 *
 * <p>A rule: {@code RuleConfig.of("NesSomeRule", b -> b.operand(Filter.class).anyInputs(), SomeRule::new).toRule()}.
 */
public record RuleConfig(String description, RelRule.OperandTransform operandSupplier, RelBuilderFactory relBuilderFactory,
                         Function<RuleConfig, RelOptRule> factory) implements RelRule.Config {
    public static RuleConfig of(String description, RelRule.OperandTransform operands, Function<RuleConfig, RelOptRule> factory) {
        return new RuleConfig(description, operands, RelFactories.LOGICAL_BUILDER, factory);
    }

    @Override
    public RelOptRule toRule() {
        return factory.apply(this);
    }

    @Override
    public RuleConfig withRelBuilderFactory(RelBuilderFactory relBuilderFactory) {
        return new RuleConfig(description, operandSupplier, relBuilderFactory, factory);
    }

    @Override
    public RuleConfig withDescription(String description) {
        return new RuleConfig(description, operandSupplier, relBuilderFactory, factory);
    }

    @Override
    public RuleConfig withOperandSupplier(RelRule.OperandTransform operandSupplier) {
        return new RuleConfig(description, operandSupplier, relBuilderFactory, factory);
    }
}

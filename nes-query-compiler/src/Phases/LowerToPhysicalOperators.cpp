/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/
#include <Phases/LowerToPhysicalOperators.hpp>

#include <algorithm>
#include <memory>
#include <ranges>
#include <string>
#include <utility>
#include <vector>
#include <LoweringRules/AbstractLoweringRule.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Traits/JoinImplementationTypeTrait.hpp>
#include <Traits/Trait.hpp>
#include <Traits/TraitSet.hpp>
#include <ErrorHandling.hpp>
#include <LoweringRuleRegistry.hpp>
#include <PhysicalOperator.hpp>
#include <PhysicalPlan.hpp>
#include <PhysicalPlanBuilder.hpp>

namespace NES::LowerToPhysicalOperators
{

namespace
{
std::unique_ptr<AbstractLoweringRule>
resolveLoweringRule(const LogicalOperator& logicalOperator, const LoweringRuleRegistryArguments& registryArgument)
{
    const auto logicalOperatorName = logicalOperator.getName();
    if (logicalOperatorName == "Join")
    {
        const auto traitSet = logicalOperator.getTraitSet();
        const auto implementationTraitOpt = getTrait<JoinImplementationTypeTrait>(traitSet);
        PRECONDITION(implementationTraitOpt.has_value(), "Join operator must have an implementation type trait");
        switch (const auto& implementationTrait = implementationTraitOpt.value(); implementationTrait->implementationType)
        {
            case JoinImplementation::HASH_JOIN: {
                if (const auto rule = LoweringRuleRegistry::instance().find(std::string("HashJoin")))
                {
                    return (*rule)(registryArgument);
                }
                throw UnknownOptimizerRule("Lowering rule for logical operator '{}' can't be resolved", logicalOperator.getName());
            }
            case JoinImplementation::NESTED_LOOP_JOIN: {
                if (const auto rule = LoweringRuleRegistry::instance().find(std::string("NLJoin")))
                {
                    return (*rule)(registryArgument);
                }
                throw UnknownOptimizerRule("Lowering rule for logical operator '{}' can't be resolved", logicalOperator.getName());
            }
            case JoinImplementation::CHOICELESS: {
                throw UnknownOptimizerRule("ImplementationTrait cannot be choiceless for join", logicalOperator.getName());
            }
        }
    }
    if (const auto rule = LoweringRuleRegistry::instance().find(std::string(logicalOperatorName)))
    {
        return (*rule)(registryArgument);
    }
    throw UnknownOptimizerRule("Lowering rule for logical operator '{}' can't be resolved", logicalOperator.getName());
}
}

LoweringRuleResultSubgraph::SubGraphRoot lowerOperatorRecursively(
    const LogicalOperator& logicalOperator,
    const std::optional<LogicalOperator>& donorOperator,
    const LoweringRuleRegistryArguments& registryArgument)
{
    if (donorOperator)
    {
        INVARIANT(logicalOperator.getName() == donorOperator->getName(), "Migration changed logical operator topology");
    }
    /// Try to resolve lowering rule for the current logical operator
    const auto rule = resolveLoweringRule(logicalOperator, registryArgument);

    /// We apply the rule and receive a subgraph
    const auto [root, leaves] = rule->applyWithDonor(logicalOperator, donorOperator);
    INVARIANT(
        leaves.size() == logicalOperator.getChildren().size(),
        "Number of children after lowering must remain the same. {}, before:{}, after:{}",
        logicalOperator,
        logicalOperator.getChildren().size(),
        leaves.size());
    /// if the lowering result is empty we bypass the operator
    if (not root)
    {
        if (not logicalOperator.getChildren().empty())
        {
            INVARIANT(
                logicalOperator.getChildren().size() == 1,
                "Empty lowering results of operators with multiple keys are not supported for {}",
                logicalOperator);
            const auto donorChildren = donorOperator ? donorOperator->getChildren() : std::vector<LogicalOperator>{};
            INVARIANT(not donorOperator || donorChildren.size() == 1, "Migration changed logical operator topology");
            return lowerOperatorRecursively(
                logicalOperator.getChildren()[0], donorOperator ? std::optional{donorChildren[0]} : std::nullopt, registryArgument);
        }
        return {};
    }
    /// We embed the subgraph into the resulting plan of physical operator wrappers
    auto children = logicalOperator.getChildren();
    const auto donorChildren = donorOperator ? donorOperator->getChildren() : std::vector<LogicalOperator>{};
    INVARIANT(not donorOperator || donorChildren.size() == children.size(), "Migration changed logical operator topology");
    INVARIANT(
        children.size() == leaves.size(),
        "Leaf node size does not match logical plan {} vs physical plan: {} for {}",
        children.size(),
        leaves.size(),
        logicalOperator);

    for (size_t index = 0; index < children.size(); ++index)
    {
        auto rootNodeOfLoweredChild = lowerOperatorRecursively(
            children[index], donorOperator ? std::optional{donorChildren[index]} : std::nullopt, registryArgument);
        leaves[index]->addChild(rootNodeOfLoweredChild);
    }
    return root;
}

PhysicalPlan
apply(const LogicalPlan& queryPlan, const QueryExecutionConfiguration& conf, const std::optional<LogicalPlan>& donorQueryPlan) /// NOLINT
{
    const auto registryArgument = LoweringRuleRegistryArguments{conf};
    const auto donorRoots = donorQueryPlan ? donorQueryPlan->getRootOperators() : std::vector<LogicalOperator>{};
    const auto roots = queryPlan.getRootOperators();
    INVARIANT(not donorQueryPlan || roots.size() == donorRoots.size(), "Migration changed logical plan roots");
    std::vector<std::shared_ptr<PhysicalOperatorWrapper>> newRootOperators;
    newRootOperators.reserve(roots.size());
    for (size_t index = 0; index < roots.size(); ++index)
    {
        newRootOperators.push_back(
            lowerOperatorRecursively(roots[index], donorQueryPlan ? std::optional{donorRoots[index]} : std::nullopt, registryArgument));
    }

    INVARIANT(not newRootOperators.empty(), "Plan must have at least one root operator");
    auto physicalPlanBuilder = PhysicalPlanBuilder(queryPlan.getQueryId());
    physicalPlanBuilder.addSinkRoot(newRootOperators[0]);
    physicalPlanBuilder.setExecutionMode(conf.executionMode.getValue());
    physicalPlanBuilder.setOperatorBufferSize(conf.operatorBufferSize.getValue());
    return std::move(physicalPlanBuilder).finalize();
}
}

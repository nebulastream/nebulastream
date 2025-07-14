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
#include <Configurations/Worker/QueryOptimizerConfiguration.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <RewriteRules/AbstractRewriteRule.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalOperator.hpp>
#include <PhysicalPlan.hpp>
#include <PhysicalPlanBuilder.hpp>
#include <RewriteRuleRegistry.hpp>


namespace NES
{

LowerToPhysicalOperators::LowerToPhysicalOperators(NES::Configurations::QueryOptimizerConfiguration queryOptimizerConfiguration)
    : queryOptimizerConfiguration(std::move(queryOptimizerConfiguration))
{
}

RewriteRuleResultSubgraph::SubGraphRoot LowerToPhysicalOperators::lowerOperatorRecursively(
    const LogicalOperator& logicalOperator, const RewriteRuleRegistryArguments& registryArgument)
{
    /// Try to resolve rewrite rule for the current logical operator
    const auto rule = [joinStrategy = queryOptimizerConfiguration.joinStrategy.getValue()](
                          const LogicalOperator& logicalOperator, const RewriteRuleRegistryArguments& registryArgument)
    {
        const auto logicalOperatorName = logicalOperator.getName();
        if (logicalOperatorName == "Join")
        {
            switch (joinStrategy)
            {
                case NES::Configurations::StreamJoinStrategy::NESTED_LOOP_JOIN: {
                    if (auto ruleOptional = RewriteRuleRegistry::instance().create(std::string("NLJoin"), registryArgument))
                    {
                        return std::move(ruleOptional.value());
                    }
                    throw UnknownOptimizerRule("Rewrite rule for logical operator '{}' can't be resolved", logicalOperator.getName());
                }
                case NES::Configurations::StreamJoinStrategy::HASH_JOIN: {
                    if (auto ruleOptional = RewriteRuleRegistry::instance().create(std::string("HashJoin"), registryArgument))
                    {
                        return std::move(ruleOptional.value());
                    }
                    throw UnknownOptimizerRule("Rewrite rule for logical operator '{}' can't be resolved", logicalOperator.getName());
                }
            }
        }

        if (auto ruleOptional = RewriteRuleRegistry::instance().create(std::string(logicalOperatorName), registryArgument))
        {
            return std::move(ruleOptional.value());
        }
        throw UnknownOptimizerRule("Rewrite rule for logical operator '{}' can't be resolved", logicalOperator.getName());
    }(logicalOperator, registryArgument);
    /// We apply the rule and receive a subgraph
    const auto [root, leafs] = rule->apply(logicalOperator);
    INVARIANT(
        leafs.size() == logicalOperator.getChildren().size(),
        "Number of children after lowering must remain the same. {}, before:{}, after:{}",
        logicalOperator,
        logicalOperator.getChildren().size(),
        leafs.size());
    /// if the lowering result is empty we bypass the operator
    if (not root)
    {
        if (not logicalOperator.getChildren().empty())
        {
            INVARIANT(
                logicalOperator.getChildren().size() == 1,
                "Empty lowering results of operators with multiple keys are not supported for {}",
                logicalOperator);
            return lowerOperatorRecursively(logicalOperator.getChildren()[0], registryArgument);
        }
        return {};
    }
    /// We embed the subgraph into the resulting plan of physical operator wrappers
    auto children = logicalOperator.getChildren();
    INVARIANT(
        children.size() == leafs.size(),
        "Leaf node size does not match logical plan {} vs physical plan: {} for {}",
        children.size(),
        leafs.size(),
        logicalOperator);

    std::ranges::for_each(
        std::views::zip(children, leafs),
        [&registryArgument, this](const auto& zippedPair)
        {
            const auto& [child, leaf] = zippedPair;
            auto rootNodeOfLoweredChild = lowerOperatorRecursively(child, registryArgument);
            leaf->addChild(rootNodeOfLoweredChild);
        });
    return root;
}

PhysicalPlan LowerToPhysicalOperators::apply(const LogicalPlan& queryPlan) /// NOLINT
{
    const auto registryArgument = RewriteRuleRegistryArguments{queryOptimizerConfiguration};
    std::vector<std::shared_ptr<PhysicalOperatorWrapper>> newRootOperators;
    newRootOperators.reserve(queryPlan.rootOperators.size());
    for (const auto& logicalRoot : queryPlan.rootOperators)
    {
        newRootOperators.push_back(lowerOperatorRecursively(logicalRoot, registryArgument));
    }

    INVARIANT(not newRootOperators.empty(), "Plan must have at least one root operator");
    auto physicalPlanBuilder = PhysicalPlanBuilder(queryPlan.getQueryId());
    physicalPlanBuilder.addSinkRoot(newRootOperators[0]);
    physicalPlanBuilder.setExecutionMode(queryOptimizerConfiguration.executionMode.getValue());
    physicalPlanBuilder.setOperatorBufferSize(queryOptimizerConfiguration.operatorBufferSize.getValue());
    return std::move(physicalPlanBuilder).finalize();
}
}

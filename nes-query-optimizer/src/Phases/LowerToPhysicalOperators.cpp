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

#include <cstddef>
#include <memory>
#include <string>
#include <vector>
#include <Operators/LogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalPlan.hpp>
#include <RewriteRuleRegistry.hpp>

namespace NES::Optimizer::LowerToPhysicalOperators
{

RewriteRuleResultSubgraph::SubGraphRoot
lowerOperatorRecursively(const LogicalOperator& logicalOperator, const RewriteRuleRegistryArguments& registryArgument)
{
    /// Try to resolve rewrite rule for the current logical operator
    auto ruleOptional = RewriteRuleRegistry::instance().create(std::string(logicalOperator.getName()), registryArgument);

    /// We expect that in this last phase of query optimiation only logical operator with physical lowering rules
    /// are part of the plan. If not the case, this indicates a bug in our query optimizer.
    INVARIANT(ruleOptional.has_value(), "Rewrite rule for logical operator '{}' can't be resolved", logicalOperator.getName());

    /// We apply the rule and receive a subgraph
    auto loweringResultSubgraph = ruleOptional.value()->apply(logicalOperator);
    INVARIANT(
        loweringResultSubgraph.leafs.size() == logicalOperator.getChildren().size(),
        "Number of children after lowering must remain the same. {}, before:{}, after:{}",
        logicalOperator,
        logicalOperator.getChildren().size(),
        loweringResultSubgraph.leafs.size());

    /// if the lowering result is empty we bypass the operator
    if (not loweringResultSubgraph.root)
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
        children.size() == loweringResultSubgraph.leafs.size(),
        "Leaf node size does not match logical plan {} vs physical plan: {} for {}",
        children.size(),
        loweringResultSubgraph.leafs.size(),
        logicalOperator);
    for (size_t i = 0; i < children.size(); ++i)
    {
        auto rootNodeOfLoweredChild = lowerOperatorRecursively(children[i], registryArgument);
        loweringResultSubgraph.leafs[i]->children.push_back(rootNodeOfLoweredChild);
    }
    return loweringResultSubgraph.root;
}

PhysicalPlan apply(LogicalPlan queryPlan, NES::Configurations::QueryOptimizerConfiguration conf)
{
    const auto registryArgument = RewriteRuleRegistryArguments{conf};
    std::vector<std::shared_ptr<PhysicalOperatorWrapper>> newRootOperators;
    for (const auto& logicalRoot : queryPlan.rootOperators)
    {
        newRootOperators.push_back(lowerOperatorRecursively(logicalRoot, registryArgument));
    }

    INVARIANT(newRootOperators.size() >= 1, "Plan must have at least one root operator");
    auto physicalPlan = PhysicalPlan(queryPlan.queryId, std::move(newRootOperators));
    return physicalPlan;
}
}

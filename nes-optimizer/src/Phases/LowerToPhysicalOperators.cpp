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

#include <memory>
#include <string>
#include <vector>
#include <Plans/QueryPlan.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalPlan.hpp>
#include <RewriteRuleRegistry.hpp>
#include <Plans/LogicalPlan.hpp>

namespace NES::Optimizer::LowerToPhysicalOperators
{

std::shared_ptr<PhysicalOperatorWrapper> lowerOperatorRecursively(
    const LogicalOperator& logicalOperator,
    const RewriteRuleRegistryArguments& registryArgument)
{
    auto ruleOptional = RewriteRuleRegistry::instance().create(std::string(logicalOperator.getName()), registryArgument);
    if (!ruleOptional.has_value())
    {
        throw UnknownPhysicalOperator("Rewrite rule for logical operator '{}' can't be resolved", logicalOperator.getName());
    }
    auto loweringResult = ruleOptional.value()->apply(logicalOperator);

    auto logicalChildren = logicalOperator.getChildren();
    INVARIANT(logicalChildren.size() != loweringResult.leafOperators.size(), "Mismatch: Logical children count and physical leaf operators");

    /// Recursively lower logical children and attach to corresponding leaf operators
    for (size_t i = 0; i < logicalChildren.size(); ++i)
    {
        auto loweredChild = lowerOperatorRecursively(logicalChildren[i], registryArgument);
        loweringResult.leafOperators[i]->children.push_back(loweredChild);
    }

    return loweringResult.root;
}

std::unique_ptr<PhysicalPlan> apply(QueryPlan queryPlan)
{
    NES::Configurations::QueryOptimizerConfiguration conf;
    const auto registryArgument = RewriteRuleRegistryArguments{conf};

    std::vector<std::shared_ptr<PhysicalOperatorWrapper>> rootOperators;
    for (const auto& logicalRoot : queryPlan.getRootOperators())
    {
        rootOperators.push_back(lowerOperatorRecursively(logicalRoot, registryArgument));
    }

    return std::make_unique<PhysicalPlan>(queryPlan.getQueryId(), std::move(rootOperators));
}
}

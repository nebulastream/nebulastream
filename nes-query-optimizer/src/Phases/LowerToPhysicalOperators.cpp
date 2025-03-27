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
#include <Operators/LogicalOperator.hpp>
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
    auto ruleOptional = RewriteRuleRegistry::instance().create(
        std::string(logicalOperator.getName()), registryArgument);
    if (!ruleOptional.has_value())
    {
        throw UnknownPhysicalOperator("Rewrite rule for logical operator '{}' can't be resolved",
                                      logicalOperator.getName());
    }

    auto loweringResult = ruleOptional.value()->apply(logicalOperator);
    auto logicalChildren = logicalOperator.getChildren();

    for (size_t i = 0; i < logicalChildren.size(); ++i)
    {
        auto loweredChild = lowerOperatorRecursively(logicalChildren[i], registryArgument);
        if (i < loweringResult.leafOperators.size() && loweringResult.leafOperators[i])
        {
            loweringResult.leafOperators[i]->children.push_back(loweredChild);
        }
        else if (loweringResult.root)
        {
            loweringResult.root->children.push_back(loweredChild);
        }
    }

    /// if the lowering result is empty e.g. for projection we bypass the operator
    if (not loweringResult.root)
    {
        if (!logicalChildren.empty())
        {
            INVARIANT(logicalChildren.size() == 1, "Empty lowering results of operators with multiple keys are not supported");
            return lowerOperatorRecursively(logicalChildren[0], registryArgument);
        }
        else
        {
            return nullptr;
        }
    }
    return loweringResult.root;
}

std::unique_ptr<PhysicalPlan> apply(LogicalPlan queryPlan)
{
    NES::Configurations::QueryOptimizerConfiguration conf;
    const auto registryArgument = RewriteRuleRegistryArguments{conf};

    std::vector<std::shared_ptr<PhysicalOperatorWrapper>> rootOperators;
    for (const auto& logicalRoot : queryPlan.getRootOperators())
    {
        rootOperators.push_back(lowerOperatorRecursively(logicalRoot, registryArgument));
    }

    INVARIANT(rootOperators.size() >= 1, "Plan must have at least one root operator");
    return std::make_unique<PhysicalPlan>(queryPlan.getQueryId(), std::move(rootOperators));
}
}

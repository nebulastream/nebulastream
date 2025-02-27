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
#include <Iterators/BFSIterator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/QueryPlan.hpp>
#include <Util/Common.hpp>
#include <ErrorHandling.hpp>
#include <RewriteRuleRegistry.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Plans/Operator.hpp>

namespace NES::Optimizer::LowerToPhysicalOperators
{

std::unique_ptr<QueryPlan> apply(const std::unique_ptr<QueryPlan> queryPlan)
{
    Operator *rootOperator = nullptr;
    Operator *currentOperator;

    INVARIANT(queryPlan->getRootOperators().size() == 1, "For now, we only support query plans with a single sink.");
    auto& root = queryPlan->getRootOperators()[0];

    for (const auto& operatorNode : BFSRange<Operator>(root))
    {
        if (dynamic_cast<SinkLogicalOperator*>(operatorNode))
        {
            rootOperator = dynamic_cast<SinkLogicalOperator*>(operatorNode);
            currentOperator = rootOperator;
        }
        else if (dynamic_cast<SourceDescriptorLogicalOperator*>(operatorNode))
        {
            const auto& tmp = currentOperator;
            currentOperator = dynamic_cast<SourceDescriptorLogicalOperator*>(operatorNode);
            tmp->children.push_back(currentOperator->clone());
        }
        else if (dynamic_cast<LogicalOperator*>(operatorNode))
        {
            auto logicalOperator = dynamic_cast<LogicalOperator*>(operatorNode);
            if (auto rule = RewriteRuleRegistry::instance().create(std::string(logicalOperator->getName()), RewriteRuleRegistryArguments{}); rule.has_value())
            {
                /// TODO here we apply the rule
                /// The problem is that we would expect that we take the TraitSet as the input
                rule.value();
                operatorNode->children.push_back(currentOperator->clone());
            }
            else
            {
                throw UnknownLogicalOperator("{} not part of RewriteRuleRegistry", logicalOperator->getName());
            }
        }
        else
        {
            throw UnknownLogicalOperator("Cannot lower {}", operatorNode->toString());
        }
    }
    std::vector<std::unique_ptr<Operator>> resultVec;
    resultVec.emplace_back(rootOperator);
    return std::make_unique<QueryPlan>(queryPlan->getQueryId(), std::move(resultVec));
}
}

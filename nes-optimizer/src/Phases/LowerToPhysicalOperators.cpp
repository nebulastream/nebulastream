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
#include <utility>
#include <vector>
#include <Iterators/BFSIterator.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/Operator.hpp>
#include <Plans/QueryPlan.hpp>
#include <Util/Common.hpp>
#include <ErrorHandling.hpp>
#include <RewriteRuleRegistry.hpp>

namespace NES::Optimizer::LowerToPhysicalOperators
{

std::unique_ptr<QueryPlan> apply(const std::unique_ptr<QueryPlan> queryPlan)
{
    std::shared_ptr<Operator> rootOperator;
    std::shared_ptr<Operator> currentOperator;

    // TODO this iterator might be wrong...
    for (const auto& operatorNode : BFSRange<Operator>(queryPlan->getRootOperators()[0]))
    {
        if (NES::Util::instanceOf<SinkLogicalOperator>(operatorNode))
        {
            rootOperator = NES::Util::as<SinkLogicalOperator>(operatorNode);
            currentOperator = rootOperator;
        }
        else if (NES::Util::instanceOf<SourceDescriptorLogicalOperator>(operatorNode))
        {
            const auto& tmp = currentOperator;
            currentOperator = NES::Util::as<SourceDescriptorLogicalOperator>(operatorNode);
            tmp->addChild(currentOperator);
        }
        else if (auto rule = RewriteRuleRegistry::instance().create(operatorNode->getName(), RewriteRuleRegistryArguments{}); rule.has_value())
        {
            /// TODO here we apply the rule
            /// The problem is that we would expect that we take the TraitSet as the input
            rule.value();
            operatorNode->addChild(currentOperator);
        }
        else
        {
            throw UnknownLogicalOperator("Cannot lower {}", operatorNode->toString());
        }
    }
    return std::make_unique<QueryPlan>(queryPlan->getQueryId(), std::vector{rootOperator});
}
}

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
#include <Operators/Sources/SourceNameLogicalOperator.hpp>
#include <Plans/QueryPlan.hpp>
#include <Util/Common.hpp>
#include <ErrorHandling.hpp>
#include <RewriteRuleRegistry.hpp>
#include <Operators/LogicalOperator.hpp>

namespace NES::Optimizer::LowerToPhysicalOperators
{

std::unique_ptr<QueryPlan> apply(std::unique_ptr<QueryPlan> queryPlan)
{
    PRECONDITION(queryPlan->getRootOperators().size() == 1, "For now, we only support query plans with a single sink.");

    for (const auto& operatorNode : BFSRange<LogicalOperator>(queryPlan->getRootOperators()[0]))
    {
        if (operatorNode.tryGet<SinkLogicalOperator>() or operatorNode.tryGet<SourceDescriptorLogicalOperator>()
            or operatorNode.tryGet<SourceNameLogicalOperator>())
        {
            continue;
        }
        //if (auto rule = RewriteRuleRegistry::instance().create(std::string(logicalOperator->getName()), RewriteRuleRegistryArguments{}); rule.has_value())
        //{
            /// TODO here we apply the rule
            /// The problem is that we would expect that we take the TraitSet as the input
        //}
        //else
        //{
        //    throw UnknownLogicalOperator("{} not part of RewriteRuleRegistry", logicalOperator->getName());
        //}
    }
    return queryPlan;
}
}

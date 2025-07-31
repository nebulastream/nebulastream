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

#include <Phases/DecideFieldOrder.hpp>
#include "Operators/ProjectionLogicalOperator.hpp"
#include "Operators/Sinks/SinkLogicalOperator.hpp"
#include "Operators/Sources/SourceDescriptorLogicalOperator.hpp"

namespace NES
{

LogicalOperator DecideFieldOrder::apply(const LogicalOperator& logicalOperator)
{
    return logicalOperator;
    const auto newChildren = logicalOperator->getChildren() | std::views::transform([this](const auto& child) { return apply(child); });

    const auto outputSchema = logicalOperator->getOutputSchema();
    std::vector<Field> orderedFields;
    if (const auto sourceOperatorOpt = logicalOperator.tryGetAs<SourceDescriptorLogicalOperator>())
    {
        const auto sourceOperator = sourceOperatorOpt.value();
        const auto physicalSchema = sourceOperator->getSourceDescriptor().getLogicalSource().getSchema();
    }

}

LogicalPlan DecideFieldOrder::apply(const LogicalPlan& queryPlan)
{
    PRECONDITION(
        std::ranges::size(queryPlan.getRootOperators()) == 1,
        "query plan must have exactly one root operator but has {}",
        std::ranges::size(queryPlan.getRootOperators()));

    const auto newRoot = apply(queryPlan.getRootOperators()[0]);
    return queryPlan.withRootOperators({newRoot});
}
}

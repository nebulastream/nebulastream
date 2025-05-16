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

#include <ranges>
#include <string>
#include <utility>
#include <LegacyOptimizer/LogicalSourceExpansionRule.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/Sources/SourceNameLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <SourceCatalogs/PhysicalSource.hpp> /// NOLINT
#include <ErrorHandling.hpp>

namespace NES::LegacyOptimizer
{

void LogicalSourceExpansionRule::apply(LogicalPlan& queryPlan) const
{
    auto sourceOperators = getOperatorByType<SourceNameLogicalOperator>(queryPlan);

    for (const auto& sourceOp : sourceOperators)
    {
        const std::string logicalSourceName(sourceOp.getLogicalSourceName());
        auto entries = sourceCatalog->getPhysicalSources(logicalSourceName);

        if (entries.empty())
        {
            throw PhysicalSourceNotFoundInQueryDescription(
                "LogicalSourceExpansionRule: Unable to find physical source locations for the logical source " + logicalSourceName);
        }

        /// Replace the SourceNameLogicalOperator with the SourceDescriptorLogicalOperator corresponding to the first entry.
        auto sourceDescriptor = entries.front()->getPhysicalSource()->createSourceDescriptor(sourceOp.getSchema());
        const LogicalOperator firstOperatorSourceLogicalDescriptor(SourceDescriptorLogicalOperator(std::move(sourceDescriptor)));
        auto replaceResult = replaceOperator(queryPlan, sourceOp, firstOperatorSourceLogicalDescriptor);
        INVARIANT(replaceResult.has_value(), "replaceOperator failed");
        queryPlan = std::move(replaceResult.value());

        /// Iterate over all subsequent entries, create the corresponding SourceDescriptorLogicalOperators and add them to the query plan.
        for (const auto& entry : entries | std::views::drop(1))
        {
            auto desc = entry->getPhysicalSource()->createSourceDescriptor(sourceOp.getSchema());
            auto nextOp = LogicalOperator{SourceDescriptorLogicalOperator(std::move(desc))};

            for (const auto& parent : getParents(queryPlan, firstOperatorSourceLogicalDescriptor))
            {
                auto children = parent.getChildren();
                children.push_back(nextOp);
                auto newParent = parent.withChildren(std::move(children));
                auto replaceResult = replaceSubtree(queryPlan, parent, newParent);
                INVARIANT(replaceResult.has_value(), "replaceOperator failed");
                queryPlan = std::move(replaceResult.value());
            }
        }
    }
}

}

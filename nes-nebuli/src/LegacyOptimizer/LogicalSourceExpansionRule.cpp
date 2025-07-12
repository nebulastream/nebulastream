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

#include <LegacyOptimizer/LogicalSourceExpansionRule.hpp>

#include <ranges>
#include <string>
#include <utility>
#include <vector>

#include <Operators/LogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/Sources/SourceNameLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Util/PlanRenderer.hpp>
#include <ErrorHandling.hpp>

namespace NES::LegacyOptimizer
{

void LogicalSourceExpansionRule::apply(LogicalPlan& queryPlan) const
{
    auto sourceOperators = getOperatorByType<SourceNameLogicalOperator>(queryPlan);

    for (const auto& sourceOp : sourceOperators)
    {
        const auto logicalSourceOpt = sourceCatalog->getLogicalSource(sourceOp.getLogicalSourceName());
        if (not logicalSourceOpt.has_value())
        {
            throw UnknownSource("{}", sourceOp.getLogicalSourceName());
        }
        const auto& logicalSource = logicalSourceOpt.value();
        const auto entriesOpt = sourceCatalog->getPhysicalSources(logicalSource);

        if (not entriesOpt.has_value())
        {
            throw UnknownSource("Source \"{}\" was removed concurrently", sourceOp.getLogicalSourceName());
        }
        const auto& entries = entriesOpt.value();
        if (entries.empty())
        {
            throw UnknownSource("No physical sources present for logical source \"{}\"", sourceOp.getLogicalSourceName());
        }

        auto sourceDescriptorOperators = entries | std::ranges::to<std::vector>();

        /// Replace the source name logical operator with the source descriptor operators
        for (auto parents = getParents(queryPlan, sourceOp); const auto& parent : parents)
        {
            auto children = parent.getChildren()
                | std::views::filter(
                                [&sourceOp](const LogicalOperator& child)
                                {
                                    if (const auto sourceNameOp = child.tryGet<SourceNameLogicalOperator>())
                                    {
                                        return sourceNameOp->getLogicalSourceName() != sourceOp.getLogicalSourceName();
                                    }
                                    return true;
                                })
                | std::ranges::to<std::vector>();
            for (const auto& entry : entries)
            {
                children.emplace_back(SourceDescriptorLogicalOperator{entry});
            }
            auto newParent = parent.withChildren(std::move(children));
            auto replaceResult = replaceSubtree(queryPlan, parent.getId(), newParent);
            INVARIANT(
                replaceResult.has_value(),
                "Failed to replace operator {} with {}",
                parent.explain(ExplainVerbosity::Debug),
                newParent.explain(ExplainVerbosity::Debug));
            queryPlan = std::move(replaceResult.value());
        }
    }
}

}

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

#include <LegacyOptimizer/Phases/LogicalSourceExpansionPhase.hpp>

#include <ranges>
#include <utility>
#include <vector>

#include <Operators/LogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/Sources/SourceNameLogicalOperator.hpp>
#include <Operators/UnionLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Pointers.hpp>

#include <ErrorHandling.hpp>

namespace NES
{

LogicalPlan LogicalSourceExpansionPhase::apply(const LogicalPlan& inputPlan, SharedPtr<const SourceCatalog> sourceCatalog)
{
    LogicalPlan plan = inputPlan;

    for (const auto& sourceOp : getOperatorByType<SourceNameLogicalOperator>(inputPlan))
    {
        const auto logicalSourceOpt = sourceCatalog->getLogicalSource(sourceOp.getLogicalSourceName());
        if (not logicalSourceOpt.has_value())
        {
            throw UnknownSourceName("{}", sourceOp.getLogicalSourceName());
        }
        const auto& logicalSource = logicalSourceOpt.value();
        const auto entriesOpt = sourceCatalog->getPhysicalSources(logicalSource);

        if (not entriesOpt.has_value())
        {
            throw UnknownSourceName("Source \"{}\" was removed concurrently", sourceOp.getLogicalSourceName());
        }
        const auto& entries = entriesOpt.value();
        if (entries.empty())
        {
            throw UnknownSourceName("No physical sources present for logical source \"{}\"", sourceOp.getLogicalSourceName());
        }

        auto expandedSourceOperators = entries
            | std::views::transform([](const auto& entry) { return LogicalOperator{SourceDescriptorLogicalOperator{entry}}; })
            | std::ranges::to<std::vector>();

        INVARIANT(getParents(plan, sourceOp).size() == 1, "Source name operator must have exactly one parent");
        auto parent = getParents(plan, sourceOp).front();
        INVARIANT(
            parent.getChildren().size() == 1 && parent.getChildren().front() == sourceOp,
            "Parent of source name operator must have exactly one child, the source itself");

        auto newParent = parent.withChildren({UnionLogicalOperator{}.withChildren(std::move(expandedSourceOperators))});
        auto replaceResult = replaceSubtree(plan, parent.getId(), newParent);

        INVARIANT(
            replaceResult.has_value(),
            "Failed to replace operator {} with {}",
            parent.explain(ExplainVerbosity::Debug),
            newParent.explain(ExplainVerbosity::Debug));
        plan = std::move(replaceResult.value());
    }
    return plan;
}

}

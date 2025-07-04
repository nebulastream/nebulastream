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
#include "Operators/UnionLogicalOperator.hpp"

namespace NES::LegacyOptimizer
{

/// Create a binary union tree when expanding a logical source to multiple physical sources
LogicalOperator createPhysicalSourceUnionTree(const std::unordered_set<SourceDescriptor>& descriptors)
{
    INVARIANT(descriptors.size() > 0, "The number of descriptors to union should be greater than 0");
    std::vector<LogicalOperator> vec1{};
    std::vector<LogicalOperator> vec2{};

    std::vector<LogicalOperator>* currentLevel = &vec1;
    std::vector<LogicalOperator>* nextLevel = &vec2;

    /// Create the leaf operators
    for (const auto& descriptor : descriptors)
    {
        currentLevel->emplace_back(SourceDescriptorLogicalOperator{descriptor});
    }

    if (descriptors.size() > 0)
    {
        return UnionLogicalOperator{true}.withChildren(std::move(*currentLevel));
    }
    else
    {
        return currentLevel->at(0);
    }

    /// Build tree levels bottom-up until only a single root operator is left
    while (currentLevel->size() >= 2)
    {
        /// Process current tree level from right to left, unioning sibilings
        while (currentLevel->size() >= 2)
        {
            LogicalOperator leftChild = std::move(currentLevel->back());
            currentLevel->pop_back();
            LogicalOperator rightChild = std::move(currentLevel->back());
            currentLevel->pop_back();

            LogicalOperator const unionOp = UnionLogicalOperator{};
            nextLevel->emplace_back(unionOp.withChildren(std::vector{std::move(leftChild), std::move(rightChild)}));
        }
        if (currentLevel->size() == 1)
        {
            /// No available union partner on this level; pass on to the next
            nextLevel->emplace_back(std::move(currentLevel->back()));
            currentLevel->pop_back();
        }
        std::swap(nextLevel, currentLevel);
    }
    INVARIANT(currentLevel->size() == 1, "There should be a root operator left after building the union tree.");
    return std::move(currentLevel->front());
}

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
                //children.emplace_back(SourceDescriptorLogicalOperator{entry});
            }
            children.emplace_back(createPhysicalSourceUnionTree(entries));

            auto newParent = parent.withChildren(std::move(children));
            auto replaceResult = replaceSubtree(queryPlan, parent, newParent);
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

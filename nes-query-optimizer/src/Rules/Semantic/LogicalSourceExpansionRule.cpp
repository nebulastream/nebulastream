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

#include <Rules/Semantic/LogicalSourceExpansionRule.hpp>

#include <ranges>
#include <utility>
#include <vector>

#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/Sources/SourceNameLogicalOperator.hpp>
#include <Operators/UnionLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Barriers/SemanticAnalysisBarrier.hpp>
#include <Rules/PlanVisitor.hpp>
#include <Util/PlanRenderer.hpp>
#include <Catalog.hpp>
#include <ErrorHandling.hpp>
#include <PlanRuleRegistry.hpp>

namespace NES
{

LogicalOperator
LogicalSourceExpansionRule::expandLogicalSource(const LogicalOperator& visiting, std::vector<LogicalOperator> children) const
{
    if (const auto sourceNameOperator = visiting.tryGetAs<SourceNameLogicalOperator>())
    {
        /// The catalog keys sources by their canonical spelling, so the lookup has to fold the name the same way.
        const auto logicalSource = catalog->getLogicalSource(sourceNameOperator.value()->getLogicalSourceName().asCanonicalString());
        const auto physicalSources = catalog->getPhysicalSources(logicalSource.getLogicalSourceName().asCanonicalString());
        if (physicalSources.empty())
        {
            throw UnknownSourceName(
                "No physical sources present for logical source \"{}\"", sourceNameOperator.value()->getLogicalSourceName());
        }
        if (std::ranges::size(children) != 0 && std::ranges::size(physicalSources) != 1)
        {
            throw UnknownSourceName("LogicalSource must either have no children or only expand to one physical source");
        }

        auto expandedSourceOperators = physicalSources
            | std::views::transform([&children](const auto& entry) -> LogicalOperator
                                    { return SourceDescriptorLogicalOperator::create(entry).withChildrenUnsafe(children); })
            | std::ranges::to<std::vector>();

        return UnionLogicalOperator::create().withChildrenUnsafe(std::move(expandedSourceOperators));
    }
    return visiting->withChildrenUnsafe(std::move(children));
}

LogicalPlan LogicalSourceExpansionRule::apply(const LogicalPlan& queryPlan) const
{
    PlanVisitor<> visitor{
        [this](const LogicalOperator& op, std::vector<LogicalOperator> children) -> PlanVisitor<>::UpResult
        { return this->expandLogicalSource(op, std::move(children)); }};

    return visitor.apply(queryPlan);
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> LogicalSourceExpansionRule::neededBy() const
{
    return {typeid(SemanticAnalysisBarrier)};
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param)
PlanRuleRegistryReturnType LogicalSourceExpansionRule::create(PlanRuleRegistryArguments arguments)
{
    return LogicalSourceExpansionRule{arguments.catalog};
}
}

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
#include <set>
#include <string_view>
#include <typeindex>
#include <typeinfo>
#include <utility>
#include <vector>

#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/Sources/SourceNameLogicalOperator.hpp>
#include <Operators/UnionLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Util/PlanRenderer.hpp>
#include <ErrorHandling.hpp>
#include <PlanRewriteUtils.hpp>

namespace NES
{

const std::type_info& LogicalSourceExpansionRule::getType()
{
    return typeid(LogicalSourceExpansionRule);
}

std::string_view LogicalSourceExpansionRule::getName()
{
    return NAME;
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> LogicalSourceExpansionRule::dependsOn() const
{
    return {};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> LogicalSourceExpansionRule::requiredBy() const
{
    return {};
};

bool LogicalSourceExpansionRule::operator==(const LogicalSourceExpansionRule& other) const
{
    return sourceCatalog == other.sourceCatalog;
}

namespace
{
LogicalOperator
expandLogicalSource(const LogicalOperator& visiting, std::vector<LogicalOperator> children, const SourceCatalog& sourceCatalog)
{
    if (const auto sourceNameOperator = visiting.tryGetAs<SourceNameLogicalOperator>())
    {
        const auto logicalSourceOpt = sourceCatalog.getLogicalSource(sourceNameOperator.value()->getLogicalSourceName());
        if (not logicalSourceOpt.has_value())
        {
            throw UnknownSourceName("{}", sourceNameOperator.value()->getLogicalSourceName());
        }
        const auto& logicalSource = logicalSourceOpt.value();
        const auto entriesOpt = sourceCatalog.getPhysicalSources(logicalSource);

        if (not entriesOpt.has_value())
        {
            throw UnknownSourceName("Source \"{}\" was removed concurrently", sourceNameOperator.value()->getLogicalSourceName());
        }
        const auto& entries = entriesOpt.value();
        if (entries.empty())
        {
            throw UnknownSourceName(
                "No physical sources present for logical source \"{}\"", sourceNameOperator.value()->getLogicalSourceName());
        }
        if (std::ranges::size(children) != 0 && std::ranges::size(entries) != 1)
        {
            throw UnknownSourceName("LogicalSource must either have no children or only expand to one physical source");
        }

        auto expandedSourceOperators = entries
            | std::views::transform([&children](const auto& entry) -> LogicalOperator
                                    { return SourceDescriptorLogicalOperator::create(entry).withChildrenUnsafe(children); })
            | std::ranges::to<std::vector>();

        return UnionLogicalOperator::create().withChildrenUnsafe(std::move(expandedSourceOperators));
    }
    return visiting->withChildrenUnsafe(std::move(children));
}
}

LogicalPlan LogicalSourceExpansionRule::apply(const LogicalPlan& queryPlan) const
{
    PRECONDITION(not queryPlan.getRootOperators().empty(), "Query must have a sink root operator");
    return rewritePlanBottomUp(
        queryPlan,
        [this](const LogicalOperator& visiting, std::vector<LogicalOperator> children)
        { return expandLogicalSource(visiting, std::move(children), *sourceCatalog); });
}
}

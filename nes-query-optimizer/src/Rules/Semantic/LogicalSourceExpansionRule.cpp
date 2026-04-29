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
#include <Util/PlanRenderer.hpp>
#include <ErrorHandling.hpp>
#include <PlannerContext.hpp>

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

bool LogicalSourceExpansionRule::operator==(const LogicalSourceExpansionRule & /*other*/) const {
    return true;
}

namespace
{
LogicalOperator applyRecursive(const LogicalOperator &visiting, const PlannerContext &ctx) {
    auto children = visiting.getChildren() | std::views::transform([&ctx](const auto &child) {
                        return applyRecursive(child, ctx);
                    })
                    | std::ranges::to<std::vector>();
    if (const auto sourceNameOperator = visiting.tryGetAs<SourceNameLogicalOperator>())
    {
        /// The catalog keys sources by their canonical spelling, so the lookup has to fold the name the same way.
        const auto logicalSource
                = SourceCatalog::getLogicalSource(
                    ctx, sourceNameOperator.value()->getLogicalSourceName().asCanonicalString());
        const auto physicalSources = SourceCatalog::getPhysicalSources(
            ctx, logicalSource.getLogicalSourceName().asCanonicalString());
        if (physicalSources.empty()) {
            throw UnknownSourceName(
                "No physical sources present for logical source \"{}\"", sourceNameOperator.value()->getLogicalSourceName());
        }
        if (std::ranges::size(children) != 0 && std::ranges::size(physicalSources) != 1) {
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
}

LogicalPlan LogicalSourceExpansionRule::apply(const LogicalPlan& queryPlan) const
{
    PRECONDITION(queryPlan.getRootOperators().size() == 1, "Query plan must have exactly one root operator");
    return queryPlan.withRootOperators({applyRecursive(queryPlan.getRootOperators().at(0), ctx)});
}
}

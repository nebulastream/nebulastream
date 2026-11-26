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

#include <algorithm>
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
#include <Rules/Barriers/SemanticAnalysisBarrier.hpp>
#include <Rules/PlanVisitor.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>
#include <PlanRuleRegistry.hpp>
#include <QuerySourceCatalog.hpp>

namespace NES
{

namespace
{
/// Gives every operator of the tree a new id, so that a fragment that is inlined more than once does not produce duplicate ids.
LogicalOperator withNewOperatorIds(const LogicalOperator& op)
{
    auto children = op.getChildren() | std::views::transform(withNewOperatorIds) | std::ranges::to<std::vector>();
    return op.withChildrenUnsafe(std::move(children)).withNewOperatorId();
}
}

LogicalOperator LogicalSourceExpansionRule::expandLogicalSource(
    const LogicalOperator& visiting, std::vector<LogicalOperator> children, std::vector<Identifier>& expansionStack) const
{
    if (const auto sourceNameOperator = visiting.tryGetAs<SourceNameLogicalOperator>())
    {
        const auto& logicalSourceName = sourceNameOperator.value()->getLogicalSourceName();
        if (std::ranges::contains(expansionStack, logicalSourceName))
        {
            throw QueryInvalid("Query sources form a cycle: {} -> {}", fmt::join(expansionStack, " -> "), logicalSourceName);
        }

        const auto logicalSourceOpt = sourceCatalog->getLogicalSource(logicalSourceName);
        if (not logicalSourceOpt.has_value())
        {
            throw UnknownSourceName("{}", logicalSourceName);
        }
        const auto& logicalSource = logicalSourceOpt.value();
        const auto entriesOpt = sourceCatalog->getPhysicalSources(logicalSource);
        if (not entriesOpt.has_value())
        {
            throw UnknownSourceName("Source \"{}\" was removed concurrently", logicalSourceName);
        }
        const auto& entries = entriesOpt.value();
        const auto fragments = querySourceCatalog ? querySourceCatalog->getQuerySources(logicalSourceName) : std::vector<LogicalOperator>{};
        const auto memberCount = std::ranges::size(entries) + std::ranges::size(fragments);
        if (memberCount == 0)
        {
            throw UnknownSourceName("No physical sources present for logical source \"{}\"", logicalSourceName);
        }
        if (std::ranges::size(children) != 0 && (memberCount != 1 || !fragments.empty()))
        {
            throw UnknownSourceName("LogicalSource must either have no children or only expand to one physical source");
        }

        auto members = entries
            | std::views::transform(
                           [&children](const auto& entry) -> LogicalOperator
                           { return SourceDescriptorLogicalOperator::create(entry).withChildrenUnsafe(children); })
            | std::ranges::to<std::vector>();

        expansionStack.push_back(logicalSourceName);
        for (const auto& fragment : fragments)
        {
            const auto expanded = expandPlan(LogicalPlan(INVALID_QUERY_ID, {withNewOperatorIds(fragment)}), expansionStack);
            INVARIANT(expanded.getRootOperators().size() == 1, "A query source fragment must have exactly one root");
            members.push_back(expanded.getRootOperators().front());
        }
        expansionStack.pop_back();

        return UnionLogicalOperator::create().withChildrenUnsafe(std::move(members));
    }
    return visiting->withChildrenUnsafe(std::move(children));
}

LogicalPlan LogicalSourceExpansionRule::expandPlan(const LogicalPlan& plan, std::vector<Identifier>& expansionStack) const
{
    PlanVisitor<> visitor{
        [this, &expansionStack](const LogicalOperator& op, std::vector<LogicalOperator> children) -> PlanVisitor<>::UpResult
        { return this->expandLogicalSource(op, std::move(children), expansionStack); }};
    return visitor.apply(plan);
}

LogicalPlan LogicalSourceExpansionRule::apply(const LogicalPlan& queryPlan) const
{
    std::vector<Identifier> expansionStack;
    return expandPlan(queryPlan, expansionStack);
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> LogicalSourceExpansionRule::neededBy() const
{
    return {typeid(SemanticAnalysisBarrier)};
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param)
PlanRuleRegistryReturnType LogicalSourceExpansionRule::create(PlanRuleRegistryArguments arguments)
{
    return LogicalSourceExpansionRule{arguments.sourceCatalog, arguments.querySourceCatalog};
}
}

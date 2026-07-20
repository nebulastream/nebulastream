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

#include <Rules/Semantic/ExtractMarkApplyRule.hpp>

#include <atomic>
#include <ranges>
#include <set>
#include <string_view>
#include <typeindex>
#include <typeinfo>
#include <utility>
#include <vector>

#include <DataTypes/DataTypeProvider.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/QuantifiedComparisonLogicalFunction.hpp>
#include <Functions/UnboundFieldAccessLogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Operators/InternalFieldCleanupLogicalOperator.hpp>
#include <Operators/MarkApplyLogicalOperator.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Semantic/LogicalSourceExpansionRule.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{
struct Extraction
{
    LogicalFunction function;
    LogicalOperator input;
    std::vector<Identifier> markerFields;
};

Identifier nextMarkerField()
{
    static std::atomic_uint64_t markerId = 0;
    return Identifier::parse(fmt::format("__nes_mark_apply_{}", markerId.fetch_add(1)));
}

LogicalOperator rewriteOperator(const LogicalOperator& visiting);

Extraction extractFunction(const LogicalFunction& function, LogicalOperator input)
{
    if (const auto quantified = function.tryGetAs<QuantifiedComparisonLogicalFunction>())
    {
        if (quantified.value()->getComparison() != QuantifiedComparisonLogicalFunction::Comparison::EQUALS
            || quantified.value()->getQuantifier() != QuantifiedComparisonLogicalFunction::Quantifier::ANY)
        {
            throw UnsupportedQuery("Only equality ANY quantified comparisons are currently executable");
        }

        auto probeValues = quantified.value()->getProbeValues();
        std::vector<Identifier> markerFields;
        for (auto& probe : probeValues)
        {
            auto extractedProbe = extractFunction(probe, std::move(input));
            probe = std::move(extractedProbe.function);
            input = std::move(extractedProbe.input);
            markerFields.insert(markerFields.end(), extractedProbe.markerFields.begin(), extractedProbe.markerFields.end());
        }

        const auto marker = nextMarkerField();
        auto subquery = rewriteOperator(quantified.value()->getSubqueryRoot());
        auto apply = MarkApplyLogicalOperator::create(std::move(probeValues), marker)
                         .withChildrenUnsafe({std::move(input), std::move(subquery)});
        markerFields.push_back(marker);
        return {
            .function = UnboundFieldAccessLogicalFunction{marker},
            .input = std::move(apply),
            .markerFields = std::move(markerFields)};
    }

    std::vector<LogicalFunction> rewrittenChildren;
    std::vector<Identifier> markerFields;
    for (const auto& child : function.getChildren())
    {
        auto extraction = extractFunction(child, std::move(input));
        rewrittenChildren.emplace_back(std::move(extraction.function));
        input = std::move(extraction.input);
        markerFields.insert(markerFields.end(), extraction.markerFields.begin(), extraction.markerFields.end());
    }
    return {.function = function.withChildren(rewrittenChildren), .input = std::move(input), .markerFields = std::move(markerFields)};
}

LogicalOperator cleanMarkers(LogicalOperator input, std::vector<Identifier> markerFields)
{
    if (markerFields.empty())
    {
        return input;
    }
    return InternalFieldCleanupLogicalOperator::create(std::move(markerFields)).withChildrenUnsafe({std::move(input)});
}

LogicalOperator rewriteOperator(const LogicalOperator& visiting)
{
    auto children = visiting.getChildren()
        | std::views::transform([](const LogicalOperator& child) { return rewriteOperator(child); })
        | std::ranges::to<std::vector>();

    if (const auto selection = visiting.tryGetAs<SelectionLogicalOperator>())
    {
        PRECONDITION(children.size() == 1, "Selection must have exactly one child");
        auto extraction = extractFunction(selection.value()->getPredicate(), std::move(children.front()));
        auto rewritten = SelectionLogicalOperator::create(std::move(extraction.function))
                             .withChildrenUnsafe({std::move(extraction.input)});
        return cleanMarkers(std::move(rewritten), std::move(extraction.markerFields));
    }

    if (const auto projection = visiting.tryGetAs<ProjectionLogicalOperator>())
    {
        PRECONDITION(children.size() == 1, "Projection must have exactly one child");
        auto input = std::move(children.front());
        std::vector<ProjectionLogicalOperator::UnboundProjection> rewrittenProjections;
        std::vector<Identifier> markerFields;
        for (const auto& [field, function] : projection.value()->getUnboundProjections())
        {
            auto extraction = extractFunction(function, std::move(input));
            rewrittenProjections.emplace_back(field, std::move(extraction.function));
            input = std::move(extraction.input);
            markerFields.insert(markerFields.end(), extraction.markerFields.begin(), extraction.markerFields.end());
        }
        auto rewritten = ProjectionLogicalOperator::create(
                             std::move(rewrittenProjections), ProjectionLogicalOperator::Asterisk{projection.value()->hasAsterisk()})
                             .withChildrenUnsafe({std::move(input)});
        return cleanMarkers(std::move(rewritten), std::move(markerFields));
    }

    if (const auto join = visiting.tryGetAs<JoinLogicalOperator>())
    {
        PRECONDITION(children.size() == 2, "Join must have exactly two children");
        const auto& predicate = join.value()->getJoinFunction();
        const auto containsQuantified = [](const auto& self, const LogicalFunction& function) -> bool
        {
            return function.tryGetAs<QuantifiedComparisonLogicalFunction>().has_value()
                || std::ranges::any_of(function.getChildren(), [&self](const auto& child) { return self(self, child); });
        };
        if (!containsQuantified(containsQuantified, predicate))
        {
            return visiting->withChildrenUnsafe(std::move(children));
        }
        if (isOuterJoin(join.value()->getJoinType()))
        {
            throw UnsupportedQuery("Quantified subqueries in outer join predicates require outer-row restoration");
        }

        auto candidatePairs = JoinLogicalOperator::create(
            ConstantValueLogicalFunction{DataTypeProvider::provideDataType(DataType::Type::BOOLEAN), "true"},
            join.value()->getWindowType(),
            JoinLogicalOperator::JoinType::CARTESIAN_PRODUCT,
            join.value()->getJoinTimeCharacteristics())
                                  .withChildrenUnsafe({std::move(children[0]), std::move(children[1])});
        auto extraction = extractFunction(predicate, std::move(candidatePairs));
        auto matchedPairs = SelectionLogicalOperator::create(std::move(extraction.function))
                                .withChildrenUnsafe({std::move(extraction.input)});
        return cleanMarkers(std::move(matchedPairs), std::move(extraction.markerFields));
    }

    return visiting->withChildrenUnsafe(std::move(children));
}
}

const std::type_info& ExtractMarkApplyRule::getType()
{
    return typeid(ExtractMarkApplyRule);
}

std::string_view ExtractMarkApplyRule::getName()
{
    return NAME;
}

std::set<std::type_index> ExtractMarkApplyRule::dependsOn() const
{
    return {};
}

std::set<std::type_index> ExtractMarkApplyRule::requiredBy() const
{
    return {typeid(LogicalSourceExpansionRule)};
}

LogicalPlan ExtractMarkApplyRule::apply(const LogicalPlan& queryPlan) const
{
    return queryPlan.withRootOperators(
        queryPlan.getRootOperators()
        | std::views::transform([](const LogicalOperator& root) { return rewriteOperator(root); })
        | std::ranges::to<std::vector>());
}

bool ExtractMarkApplyRule::operator==(const ExtractMarkApplyRule&) const
{
    return true;
}

}

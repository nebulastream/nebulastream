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


#include <Rules/Semantic/UnnestTranslationRule.hpp>

#include <algorithm>
#include <cstddef>
#include <optional>
#include <ranges>
#include <set>
#include <typeindex>
#include <typeinfo>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Functions/UnnestLogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/UnnestLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Barriers/SemanticAnalysisBarrier.hpp>
#include <Rules/PlanVisitor.hpp>
#include <Rules/Semantic/TypeInferenceRule.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <PlanRuleRegistry.hpp>

namespace NES
{

namespace
{

bool containsUnnest(const LogicalFunction& function)
{
    return std::ranges::any_of(
        BFSRange{function}, [](const LogicalFunction& node) { return node.tryGetAs<UnnestLogicalFunction>().has_value(); });
}

/// `function` with every UNNEST node replaced by `element`.
LogicalFunction replaceUnnest(const LogicalFunction& function, const LogicalFunction& element)
{
    if (function.tryGetAs<UnnestLogicalFunction>())
    {
        return element;
    }
    const auto children = function.getChildren();
    if (children.empty())
    {
        return function;
    }
    return function.withChildren(
        children | std::views::transform([&element](const LogicalFunction& child) { return replaceUnnest(child, element); })
        | std::ranges::to<std::vector>());
}

/// A field name the child does not have yet.
Identifier freshHelper(const LogicalOperator& child)
{
    for (size_t index = 0;; ++index)
    {
        auto name = Identifier::parse(fmt::format("__UNNEST_{}", index));
        if (!child.getOutputSchema().getFieldByName(name).has_value())
        {
            return name;
        }
    }
}

LogicalOperator translate(const ProjectionLogicalOperator& projection, const LogicalOperator& child)
{
    std::optional<UnnestLogicalFunction> unnest;
    for (const auto& [name, function] : projection.getProjections())
    {
        for (const auto& node : BFSRange{function})
        {
            const auto found = node.tryGetAs<UnnestLogicalFunction>();
            if (!found)
            {
                continue;
            }
            if (unnest && !(*unnest == found->get()))
            {
                throw UnsupportedQuery("UNNEST is supported once per SELECT list, but it unnests both {} and {}", *unnest, found->get());
            }
            if (containsUnnest(found->get().getChild()))
            {
                throw UnsupportedQuery("UNNEST of an UNNEST is not supported: {}", found->get());
            }
            unnest = found->get();
        }
    }
    INVARIANT(unnest.has_value(), "translate is called for projections with UNNEST only");

    const auto array = unnest->getChild().tryGetAs<FieldAccessLogicalFunction>();
    if (!array)
    {
        throw UnsupportedQuery("UNNEST expects a field as its argument, but got {}", unnest->getChild());
    }
    const auto helper = freshHelper(child);
    const LogicalOperator unnested = UnnestLogicalOperator::create(child, array->get().getField().getLastName(), helper);
    const LogicalFunction element = FieldAccessLogicalFunction{*unnested.getOutputSchema()[helper]};

    std::vector<ProjectionLogicalOperator::UnboundProjection> projections;
    if (projection.hasAsterisk())
    {
        for (const auto& field : child.getOutputSchema())
        {
            projections.emplace_back(field.getLastName(), FieldAccessLogicalFunction{*unnested.getOutputSchema()[field.getLastName()]});
        }
    }
    for (const auto& [name, function] : projection.getProjections())
    {
        projections.emplace_back(name.getLastName(), replaceUnnest(function, element));
    }
    return ProjectionLogicalOperator::create(unnested, std::move(projections), ProjectionLogicalOperator::Asterisk{false});
}

}

LogicalPlan UnnestTranslationRule::apply(const LogicalPlan& queryPlan) const /// NOLINT(readability-convert-member-functions-to-static)
{
    using TranslationVisitor = PlanVisitor<std::monostate, std::monostate, bool>;
    TranslationVisitor visitor{
        [](const LogicalOperator& op, std::vector<LogicalOperator> children, std::unordered_map<LogicalOperator, bool> upContexts)
            -> TranslationVisitor::UpResult
        {
            if (const auto projection = op.tryGetAs<ProjectionLogicalOperator>(); projection
                && std::ranges::any_of(
                    projection->get().getProjections(), [](const auto& entry) { return containsUnnest(entry.second); }))
            {
                PRECONDITION(children.size() == 1, "Expected a projection to have one child, but has {}", children.size());
                return {translate(projection->get(), children.front()), true};
            }
            if (const auto selection = op.tryGetAs<SelectionLogicalOperator>(); selection && containsUnnest(selection->get().getPredicate()))
            {
                throw UnsupportedQuery("UNNEST is supported in the SELECT list only, but the WHERE clause has {}", selection->get().getPredicate());
            }
            if (std::ranges::none_of(upContexts, [](const auto& entry) { return entry.second; }))
            {
                return {op, false};
            }
            return {op.withChildren(std::move(children)), true};
        }};
    return visitor.apply(queryPlan);
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> UnnestTranslationRule::needs() const
{
    return {typeid(TypeInferenceRule)};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> UnnestTranslationRule::neededBy() const
{
    return {typeid(SemanticAnalysisBarrier)};
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param)
PlanRuleRegistryReturnType UnnestTranslationRule::create(PlanRuleRegistryArguments)
{
    return UnnestTranslationRule{};
}

}

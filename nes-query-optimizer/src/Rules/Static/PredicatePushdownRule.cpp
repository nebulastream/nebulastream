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

#include <Rules/Static/PredicatePushdownRule.hpp>

#include <algorithm>
#include <ranges>
#include <set>
#include <string_view>
#include <typeindex>
#include <typeinfo>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <Functions/BooleanFunctions/AndLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Operators/EventTimeWatermarkAssignerLogicalOperator.hpp>
#include <Operators/IngestionTimeWatermarkAssignerLogicalOperator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/UnionLogicalOperator.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Operators/Windows/WindowedAggregationLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Barriers/FixedPlanStructureBarrier.hpp>
#include <Rules/Static/RedundantProjectionRemovalRule.hpp>
#include <Schema/Field.hpp>
#include <ErrorHandling.hpp>
#include <PlanRewriteUtils.hpp>

namespace NES
{

namespace
{
[[nodiscard]] LogicalOperator predicatePushdown(
    PushdownBarrier& ctx, LogicalOperator op, std::vector<LogicalFunction> predicateSet, std::unordered_map<Field, Field> fields);

std::vector<LogicalFunction> splitPredicate(LogicalFunction function)
{
    if (const auto andFunction = function.tryGetAs<AndLogicalFunction>())
    {
        const auto children = andFunction.value().getChildren();
        auto splitChildFunctions = splitPredicate(children.at(0));
        const auto rightSplitChildFunctions = splitPredicate(children.at(1));

        for (const auto& rightFunction : rightSplitChildFunctions)
        {
            splitChildFunctions.emplace_back(rightFunction);
        }
        return splitChildFunctions;
    }
    return {std::move(function)};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::vector<Field> getAccessedFields(LogicalFunction logicalFunction)
{
    return BFSRange(std::move(logicalFunction))
        | std::views::filter([](const LogicalFunction& function) { return function.tryGetAs<FieldAccessLogicalFunction>().has_value(); })
        | std::views::transform([](const LogicalFunction& function) { return function.getAs<FieldAccessLogicalFunction>()->getField(); })
        | std::ranges::to<std::vector>();
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
LogicalOperator
addSelectionIfRequired(LogicalOperator op, std::vector<LogicalFunction> predicateSet, const std::unordered_map<Field, Field>& fields)
{
    if (predicateSet.empty())
    {
        return op;
    }

    LogicalFunction predicate = std::move(predicateSet.back());
    predicateSet.pop_back();

    while (!predicateSet.empty())
    {
        predicate = AndLogicalFunction{std::move(predicate), std::move(predicateSet.back())};
        predicateSet.pop_back();
    }

    predicate = replaceFieldAccesses(predicate, fields);

    /// During inference, the function automatically updates the referenced fields to its new selection operator.
    return TypedLogicalOperator<SelectionLogicalOperator>{op, std::move(predicate)};
}

std::unordered_map<Field, Field> fieldsWithNewOperator(const LogicalOperator& base, const std::unordered_map<Field, Field>& fields)
{
    /// returns with new map from original field to new field in given base operator
    /// ignores fields that are not available in base operator.

    std::unordered_map<Field, Field> newFields;
    for (const auto& [key, value] : fields)
    {
        if (auto childField = base.getOutputSchema()[value.getLastName()])
        {
            newFields.emplace(key, childField.value());
        }
    }
    return newFields;
}

LogicalOperator applyToAllChildren(
    PushdownBarrier& ctx,
    const LogicalOperator& op,
    const std::vector<LogicalFunction>& predicateSet,
    const std::unordered_map<Field, Field>& fields)
{
    std::vector<LogicalOperator> children;
    for (const auto& child : op.getChildren())
    {
        auto newChild = predicatePushdown(ctx, child, predicateSet, fieldsWithNewOperator(child, fields));
        children.push_back(newChild);
    }
    /// withChildren re-infers only the local schema and does not recurse into the children. This relies on the
    /// children's schemas already being inferred, which holds here: the input plan is type-inferred before this
    /// rule runs, and every child was rebuilt through withChildren or a constructor that infers locally.
    /// A recursive withInferredSchema would copy the already-rewritten (potentially shared) subtrees and break
    /// operator sharing in multi-sink plans.
    return op.withChildren(children);
}

LogicalOperator pushBeyondSelection(
    PushdownBarrier& ctx,
    const TypedLogicalOperator<SelectionLogicalOperator>& op,
    std::vector<LogicalFunction> predicateSet,
    std::unordered_map<Field, Field> fields)
{
    /// Add predicate to predicate set.
    /// Predicates are cut at conjunctions.
    /// Apply predicate pushdown recursively

    fields = fieldsWithNewOperator(op->getChild(), fields);

    auto predicate = op->getPredicate();
    for (const auto& field : getAccessedFields(predicate))
    {
        fields.emplace(field, field);
    }

    for (const auto& newPredicate : splitPredicate(std::move(predicate)))
    {
        predicateSet.emplace_back(newPredicate);
    }

    return predicatePushdown(ctx, op->getChild(), std::move(predicateSet), std::move(fields));
}

LogicalOperator pushBeyondUnion(
    PushdownBarrier& ctx,
    const TypedLogicalOperator<UnionLogicalOperator>& op,
    const std::vector<LogicalFunction>& predicateSet,
    const std::unordered_map<Field, Field>& fields)
{
    /// predicates are pushed to all children

    std::vector<LogicalOperator> newChildren;
    for (const auto& child : op.getChildren())
    {
        const auto childFields = fieldsWithNewOperator(child, fields);
        newChildren.emplace_back(predicatePushdown(ctx, child, predicateSet, childFields));
    }

    return op.withChildren(newChildren);
}

LogicalOperator pushBeyondProjection(
    PushdownBarrier& ctx,
    const TypedLogicalOperator<ProjectionLogicalOperator>& op,
    const std::vector<LogicalFunction>& predicateSet,
    const std::unordered_map<Field, Field>& fields)
{
    /// identifies which fields are non created/modified in the operator, but just forwarded
    /// pushed set of predicates that only access such fields further
    /// applies the remaining predicates.

    std::unordered_set<Field> pushableFields;

    if (op->hasAsterisk())
    {
        for (const auto& childField : op->getChild().getOutputSchema())
        {
            auto field = op.getOutputSchema()[childField.getLastName()];
            INVARIANT(field.has_value(), "If projection has asterisk, all child fields must be available without renaming");
            pushableFields.emplace(field.value());
        }
    }

    for (const auto& [field, function] : op->getProjections())
    {
        if (auto fieldAccess = function.tryGetAs<FieldAccessLogicalFunction>(); fieldAccess.has_value())
        {
            if (field.getLastName() == fieldAccess.value()->getField().getLastName())
            {
                pushableFields.emplace(field);
            }
        }
    }

    std::vector<LogicalFunction> nonPushable;
    std::vector<LogicalFunction> pushable;

    for (const auto& predicate : predicateSet)
    {
        auto filteredOn = getAccessedFields(predicate);

        for (auto& field : filteredOn)
        {
            PRECONDITION(fields.contains(field), "each field accessed by the predicate must be in fields map.");
            field = fields.at(field);
        }

        const bool isPushable
            = std::ranges::all_of(filteredOn, [&pushableFields](const Field& field) { return pushableFields.contains(field); });

        if (isPushable)
        {
            pushable.emplace_back(predicate);
        }
        else
        {
            nonPushable.emplace_back(predicate);
        }
    }

    const auto newOp = applyToAllChildren(ctx, op, pushable, fields);

    return addSelectionIfRequired(newOp, std::move(nonPushable), fields);
}

LogicalOperator pushBeyondJoin(
    PushdownBarrier& ctx,
    const TypedLogicalOperator<JoinLogicalOperator>& op,
    const std::vector<LogicalFunction>& predicateSet,
    const std::unordered_map<Field, Field>& fields)
{
    /// Sorts predicate set into
    /// * predicates that only access fields from right child,
    /// * predicates that only access fields from left child,
    /// * and the remaining predicates.
    /// The predicates only accessing the left or right child are pushed accordingly
    /// and the remaining predicates are applied.

    std::vector<LogicalFunction> leftPushable;
    std::vector<LogicalFunction> rightPushable;
    std::vector<LogicalFunction> nonPushable;

    const auto [left, right] = op->getBothChildren();

    const auto leftFields = fieldsWithNewOperator(left, fields);
    const auto rightFields = fieldsWithNewOperator(right, fields);

    for (const auto& predicate : predicateSet)
    {
        const auto filteredOn = getAccessedFields(predicate);

        const bool allInLeft = std::ranges::all_of(filteredOn, [&](const Field& field) { return leftFields.contains(field); });
        const bool allInRight = std::ranges::all_of(filteredOn, [&](const Field& field) { return rightFields.contains(field); });

        if (allInLeft && !allInRight)
        {
            leftPushable.emplace_back(predicate);
        }
        else if (allInRight && !allInLeft)
        {
            rightPushable.emplace_back(predicate);
        }
        else
        {
            nonPushable.emplace_back(predicate);
        }
    }

    const auto newLeft = predicatePushdown(ctx, left, std::move(leftPushable), leftFields);
    const auto newRight = predicatePushdown(ctx, right, std::move(rightPushable), rightFields);

    const auto newOp = op->withChildren({newLeft, newRight});

    return addSelectionIfRequired(newOp, std::move(nonPushable), fields);
}

LogicalOperator pushBeyondWatermarkAssigner(
    PushdownBarrier& ctx,
    const LogicalOperator& op,
    const std::vector<LogicalFunction>& predicateSet,
    const std::unordered_map<Field, Field>& fields)
{
    /// pushes all predicates further because
    /// operator does not add/modify any fields

    const auto newFields = fieldsWithNewOperator(op->getChildren().at(0), fields);
    return applyToAllChildren(ctx, op, predicateSet, newFields);
}

LogicalOperator pushBeyondWindowedAggregation(
    PushdownBarrier& ctx,
    const TypedLogicalOperator<WindowedAggregationLogicalOperator>& op,
    const std::vector<LogicalFunction>& predicateSet,
    std::unordered_map<Field, Field> fields)
{
    /// only pushes predicates that only access grouping keys
    /// applies the remaining predicates

    const auto groupingKeys = op->getGroupingKeys();


    std::vector<LogicalFunction> pushable;
    std::vector<LogicalFunction> nonPushable;

    fields = fieldsWithNewOperator(op->getChild(), fields);

    for (const auto& predicate : predicateSet)
    {
        const auto accessedFields = getAccessedFields(predicate);
        const bool isPushable = std::ranges::all_of(
            accessedFields,
            [&](const Field& accessedField)
            {
                return std::ranges::any_of(
                    groupingKeys,
                    [&](const TypedLogicalFunction<FieldAccessLogicalFunction>& groupingKey)
                    {
                        if (const auto& found = fields.find(accessedField); found != fields.end())
                        {
                            return groupingKey->getField() == found->second;
                        }
                        return false;
                    });
            });

        if (isPushable)
        {
            pushable.emplace_back(predicate);
        }
        else
        {
            nonPushable.emplace_back(predicate);
        }
    }

    const auto newOp = applyToAllChildren(ctx, op, pushable, fields);

    return addSelectionIfRequired(newOp, std::move(nonPushable), fields);
}

LogicalOperator predicatePushdownDispatch(
    PushdownBarrier& ctx, LogicalOperator op, std::vector<LogicalFunction> predicateSet, std::unordered_map<Field, Field> fields)
{
    if (op.tryGetAs<SourceDescriptorLogicalOperator>())
    {
        return addSelectionIfRequired(std::move(op), std::move(predicateSet), fields);
    }
    if (auto selectionOp = op.tryGetAs<SelectionLogicalOperator>())
    {
        return pushBeyondSelection(ctx, selectionOp.value(), std::move(predicateSet), std::move(fields));
    }
    if (auto projectionOp = op.tryGetAs<ProjectionLogicalOperator>())
    {
        return pushBeyondProjection(ctx, projectionOp.value(), predicateSet, fields);
    }
    if (auto joinOp = op.tryGetAs<JoinLogicalOperator>())
    {
        return pushBeyondJoin(ctx, joinOp.value(), predicateSet, fields);
    }
    if (auto unionOp = op.tryGetAs<UnionLogicalOperator>())
    {
        return pushBeyondUnion(ctx, unionOp.value(), predicateSet, fields);
    }
    if (auto eventTimeOp = op.tryGetAs<EventTimeWatermarkAssignerLogicalOperator>())
    {
        return pushBeyondWatermarkAssigner(ctx, std::move(eventTimeOp.value()), predicateSet, fields);
    }
    if (auto ingestionTimeOp = op.tryGetAs<IngestionTimeWatermarkAssignerLogicalOperator>())
    {
        return pushBeyondWatermarkAssigner(ctx, std::move(ingestionTimeOp.value()), predicateSet, fields);
    }
    if (auto windowedAggOp = op.tryGetAs<WindowedAggregationLogicalOperator>())
    {
        return pushBeyondWindowedAggregation(ctx, windowedAggOp.value(), predicateSet, std::move(fields));
    }

    op = applyToAllChildren(ctx, op, {}, {});
    return addSelectionIfRequired(std::move(op), std::move(predicateSet), fields);
}

LogicalOperator predicatePushdown(
    PushdownBarrier& ctx, LogicalOperator op, std::vector<LogicalFunction> predicateSet, std::unordered_map<Field, Field> fields)
{
    /// A shared operator is a pushdown barrier: pending predicates must not enter a subtree another parent also reads, so
    /// they are materialized above it while the shared subtree itself is rewritten once with an empty predicate set.
    if (ctx.isShared(op))
    {
        return ctx.rewriteShared(
            op,
            [&] { return predicatePushdownDispatch(ctx, op, {}, {}); },
            [&](const LogicalOperator& rewritten)
            { return addSelectionIfRequired(rewritten, std::move(predicateSet), fieldsWithNewOperator(rewritten, fields)); });
    }
    return predicatePushdownDispatch(ctx, std::move(op), std::move(predicateSet), std::move(fields));
}

}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
LogicalPlan PredicatePushdownRule::apply(const LogicalPlan& queryPlan) const
{
    PRECONDITION(not queryPlan.getRootOperators().empty(), "Query must have a sink root operator");

    PushdownBarrier ctx{queryPlan};
    std::vector<LogicalOperator> newRoots;
    newRoots.reserve(queryPlan.getRootOperators().size());
    for (const auto& root : queryPlan.getRootOperators())
    {
        newRoots.push_back(predicatePushdown(ctx, root, {}, {}));
    }
    return queryPlan.withRootOperators(newRoots);
}

const std::type_info& PredicatePushdownRule::getType()
{
    return typeid(PredicatePushdownRule);
}

std::string_view PredicatePushdownRule::getName()
{
    return NAME;
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> PredicatePushdownRule::dependsOn() const
{
    return {};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> PredicatePushdownRule::requiredBy() const
{
    return {typeid(RedundantProjectionRemovalRule), typeid(FixedPlanStructureBarrier)};
}

bool PredicatePushdownRule::operator==(const PredicatePushdownRule& /*other*/) const
{
    return true;
}
}

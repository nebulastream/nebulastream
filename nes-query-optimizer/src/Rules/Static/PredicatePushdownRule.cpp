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
#include <cstddef>
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
#include <Rules/Barriers/SemanticAnalysisBarrier.hpp>
#include <Rules/PlanVisitor.hpp>
#include <Schema/Field.hpp>
#include <ErrorHandling.hpp>
#include <PlanRewriteUtils.hpp>
#include <PlanRuleRegistry.hpp>

namespace NES
{

namespace
{

struct Projections
{
    std::vector<LogicalFunction> predicateSet;
    std::unordered_map<Field, Field> fields;
};

struct OperatorContext
{
    Projections toApply;
    std::unordered_map<LogicalOperator, Projections> pushed;
    bool hasMultipleParents = false;
};

Projections merge(const std::vector<Projections>& contexts)
{
    if (contexts.empty() || contexts.size() > 1)
    {
        return {};
    }
    return contexts.at(0);
}

using Visitor = PlanVisitor<OperatorContext, Projections, bool>;

[[nodiscard]] Visitor::DownResult predicatePushdown(const LogicalOperator& op, const std::vector<Projections>& downContexts);

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

Visitor::DownResult pushBeyondSelection(
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

    std::unordered_map<LogicalOperator, Projections> toPush{{op->getChild(), {.predicateSet = predicateSet, .fields = fields}}};
    const OperatorContext operatorContext{.toApply = {}, .pushed = toPush};

    return {.operatorContext = operatorContext, .downContexts = std::move(toPush)};
}

Visitor::DownResult pushBeyondUnion(
    const TypedLogicalOperator<UnionLogicalOperator>& op,
    const std::vector<LogicalFunction>& predicateSet,
    const std::unordered_map<Field, Field>& fields)
{
    /// predicates are pushed to all children

    std::vector<LogicalOperator> newChildren;

    std::unordered_map<LogicalOperator, Projections> downContext;

    for (const auto& child : op.getChildren())
    {
        const auto childFields = fieldsWithNewOperator(child, fields);
        downContext[child] = {.predicateSet = predicateSet, .fields = childFields};
    }

    OperatorContext operatorContext{.toApply = {}, .pushed = downContext};

    return {.operatorContext = std::move(operatorContext), .downContexts = downContext};
}

Visitor::DownResult pushBeyondProjection(
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


    std::unordered_map<LogicalOperator, Projections> downContext;
    for (const auto& child : op.getChildren())
    {
        downContext[child] = {.predicateSet = pushable, .fields = fieldsWithNewOperator(child, fields)};
    }

    Projections toApply{.predicateSet = nonPushable, .fields = fields};

    OperatorContext operatorContext{.toApply = std::move(toApply), .pushed = downContext};

    return {.operatorContext = std::move(operatorContext), .downContexts = downContext};
}

Visitor::DownResult pushBeyondJoin(
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


    std::unordered_map<LogicalOperator, Projections> downContexts;

    downContexts[left] = {.predicateSet = leftPushable, .fields = leftFields};
    downContexts[right] = {.predicateSet = rightPushable, .fields = rightFields};

    Projections toApply = {.predicateSet = nonPushable, .fields = fields};

    OperatorContext operatorContext{.toApply = std::move(toApply), .pushed = downContexts};

    return {.operatorContext = std::move(operatorContext), .downContexts = downContexts};
}

Visitor::DownResult pushBeyondWatermarkAssigner(
    const LogicalOperator& op, const std::vector<LogicalFunction>& predicateSet, const std::unordered_map<Field, Field>& fields)
{
    /// pushes all predicates further because
    /// operator does not add/modify any fields

    PRECONDITION(op.getChildren().size() == 1, "WatermarkAssigners must have exactly one child");

    auto child = op.getChildren().at(0);
    auto childFields = fieldsWithNewOperator(op->getChildren().at(0), fields);

    return {.operatorContext = {}, .downContexts = {{std::move(child), {.predicateSet = predicateSet, .fields = std::move(childFields)}}}};
}

Visitor::DownResult pushBeyondWindowedAggregation(
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

    std::unordered_map<LogicalOperator, Projections> downContexts;

    downContexts[op->getChild()] = {.predicateSet = pushable, .fields = fieldsWithNewOperator(op->getChild(), fields)};

    Projections toApply = {.predicateSet = nonPushable, .fields = fields};
    OperatorContext operatorContext{.toApply = std::move(toApply), .pushed = downContexts};

    return {.operatorContext = std::move(operatorContext), .downContexts = downContexts};
}

Visitor::DownResult predicatePushdown(const LogicalOperator& op, const std::vector<Projections>& downContexts)
{
    const bool hasMultipleParents = downContexts.size() > 1;

    auto [predicateSet, fields] = merge(downContexts);

    Visitor::DownResult downResult;

    if (op.tryGetAs<SourceDescriptorLogicalOperator>())
    {
        OperatorContext operatorContext = {.toApply = {.predicateSet = predicateSet, .fields = fields}, .pushed = {}};
        downResult = {.operatorContext = std::move(operatorContext), .downContexts = {}};
    }
    else if (auto selectionOp = op.tryGetAs<SelectionLogicalOperator>())
    {
        downResult = pushBeyondSelection(selectionOp.value(), predicateSet, fields);
    }
    else if (auto projectionOp = op.tryGetAs<ProjectionLogicalOperator>())
    {
        downResult = pushBeyondProjection(projectionOp.value(), predicateSet, fields);
    }
    else if (auto joinOp = op.tryGetAs<JoinLogicalOperator>())
    {
        downResult = pushBeyondJoin(joinOp.value(), predicateSet, fields);
    }
    else if (auto unionOp = op.tryGetAs<UnionLogicalOperator>())
    {
        downResult = pushBeyondUnion(unionOp.value(), predicateSet, fields);
    }
    else if (auto eventTimeOp = op.tryGetAs<EventTimeWatermarkAssignerLogicalOperator>())
    {
        downResult = pushBeyondWatermarkAssigner(std::move(eventTimeOp.value()), predicateSet, fields);
    }
    else if (auto ingestionTimeOp = op.tryGetAs<IngestionTimeWatermarkAssignerLogicalOperator>())
    {
        downResult = pushBeyondWatermarkAssigner(std::move(ingestionTimeOp.value()), predicateSet, fields);
    }
    else if (auto windowedAggOp = op.tryGetAs<WindowedAggregationLogicalOperator>())
    {
        downResult = pushBeyondWindowedAggregation(windowedAggOp.value(), predicateSet, fields);
    }
    else
    {
        downResult
            = {.operatorContext
               = {.toApply = {.predicateSet = std::move(predicateSet), .fields = fields},
                  .pushed = {},
                  .hasMultipleParents = hasMultipleParents},
               .downContexts = {}};
    }

    downResult.operatorContext.hasMultipleParents = hasMultipleParents;


    return downResult;
}

Visitor::UpResult rebuildPlan(
    LogicalOperator op,
    std::vector<LogicalOperator> children,
    const OperatorContext& opContext,
    const std::unordered_map<LogicalOperator, bool>& childWithMultipleParents)
{
    /// Add selections infront of children if they couldn't apply predicates because they have mutliple children
    const auto originalChildren = op.getChildren();
    for (size_t i = 0; i < children.size(); ++i)
    {
        if (childWithMultipleParents.at(children[i]) && opContext.pushed.contains(originalChildren.at(i)))
        {
            auto [predicates, fields] = opContext.pushed.at(originalChildren.at(i));
            children[i] = addSelectionIfRequired(children[i], predicates, fields);
        }
    }

    if (op.tryGetAs<SelectionLogicalOperator>())
    {
        INVARIANT(children.size() == 1, "selection operators can only have one child");
        return {children.at(0), opContext.hasMultipleParents};
    }

    op = op.withChildren(children);
    return {addSelectionIfRequired(op, opContext.toApply.predicateSet, opContext.toApply.fields), opContext.hasMultipleParents};
}

}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
LogicalPlan PredicatePushdownRule::apply(const LogicalPlan& queryPlan) const
{
    Visitor visitor{predicatePushdown, rebuildPlan};
    return visitor.apply(queryPlan);
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> PredicatePushdownRule::needs() const
{
    return {typeid(SemanticAnalysisBarrier)};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> PredicatePushdownRule::neededBy() const
{
    return {typeid(FixedPlanStructureBarrier)};
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param)
PlanRuleRegistryReturnType PlanRuleGeneratedRegistrar::RegisterPredicatePushdownPlanRule(PlanRuleRegistryArguments)
{
    return PredicatePushdownRule{};
}

}

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

#include <ranges>

#include <utility>

#include <Functions/BooleanFunctions/AndLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Operators/EventTimeWatermarkAssignerLogicalOperator.hpp>
#include <Operators/IngestionTimeWatermarkAssignerLogicalOperator.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/UnionLogicalOperator.hpp>
#include <Operators/Windows/WindowedAggregationLogicalOperator.hpp>
#include <Rules/Barriers/FixedPlanStructureBarrier.hpp>

namespace NES
{

LogicalPlan PredicatePushdownRule::apply(LogicalPlan queryPlan) const
{
    /// TODO Delete Debug messages
    std::cerr << explain(queryPlan, ExplainVerbosity::Debug) << '\n';

    auto originalRoots = queryPlan.getRootOperators();
    PRECONDITION(originalRoots.size() == 1, "predicate pushdown not yet implemented for more than one root");

    auto newRoot = predicatePushdown(originalRoots.at(0), {});

    queryPlan = queryPlan.withRootOperators({newRoot});

    std::cerr << explain(queryPlan, ExplainVerbosity::Debug) << '\n';

    return queryPlan;
}

LogicalOperator PredicatePushdownRule::predicatePushdown(LogicalOperator op, std::vector<LogicalFunction> predicateSet) const
{
    if (auto sourceOp = op.tryGetAs<SourceDescriptorLogicalOperator>())
    {
        return addSelectionIfRequired(std::move(op), std::move(predicateSet));
    }
    if (auto selectionOp = op.tryGetAs<SelectionLogicalOperator>())
    {
        return pushBeyondSelection(std::move(selectionOp.value()), std::move(predicateSet));
    }
    if (auto projectionOp = op.tryGetAs<ProjectionLogicalOperator>())
    {
        return pushBeyondProjection(std::move(projectionOp.value()), std::move(predicateSet));
    }
    if (auto joinOp = op.tryGetAs<JoinLogicalOperator>())
    {
        return pushBeyondJoin(std::move(joinOp.value()), std::move(predicateSet));
    }
    if (auto unionOp = op.tryGetAs<UnionLogicalOperator>())
    {
        return pushBeyondUnion(std::move(unionOp.value()), std::move(predicateSet));
    }
    if (auto eventTimeOp = op.tryGetAs<EventTimeWatermarkAssignerLogicalOperator>())
    {
        return pushBeyondEventTimeWatermarkAssigner(std::move(eventTimeOp.value()), std::move(predicateSet));
    }
    if (auto ingestionTimeOp = op.tryGetAs<IngestionTimeWatermarkAssignerLogicalOperator>())
    {
        return pushBeyondIngestionTimeWatermarkAssigner(std::move(ingestionTimeOp.value()), std::move(predicateSet));
    }

    if (auto windowedAggOp = op.tryGetAs<WindowedAggregationLogicalOperator>())
    {
        return pushBeyondWindowedAggregation(std::move(windowedAggOp.value()), std::move(predicateSet));
    }

    op = applyToAllChildren(op, {});
    return addSelectionIfRequired(std::move(op), std::move(predicateSet));
}

LogicalOperator PredicatePushdownRule::pushBeyondSelection(
    TypedLogicalOperator<SelectionLogicalOperator> op, std::vector<LogicalFunction> predicateSet) const
{
    auto predicate = op->getPredicate();
    auto splitPredicates = splitPredicate(std::move(predicate));

    for (auto&& newPredicate : splitPredicates)
    {
        predicateSet.emplace_back(std::move(newPredicate));
    }

    return predicatePushdown(op.getChildren().at(0), predicateSet);
}

LogicalOperator
PredicatePushdownRule::pushBeyondUnion(TypedLogicalOperator<UnionLogicalOperator> op, std::vector<LogicalFunction> predicateSet) const
{
    return applyToAllChildren(std::move(op), std::move(predicateSet));
}

LogicalOperator PredicatePushdownRule::pushBeyondProjection(
    TypedLogicalOperator<ProjectionLogicalOperator> op, std::vector<LogicalFunction> predicateSet) const
{
    // TODO read with renames
    // TODO what about asterix?
    std::set<std::string> pushableFields;
    for (const auto& [identifier, function] : op->getProjections())
    {
        if (!identifier.has_value())
        {
            continue;
        }
        if (auto fieldAccess = function.tryGetAs<FieldAccessLogicalFunction>(); fieldAccess.has_value())
        {
            auto newField = identifier->getFieldName();
            auto originalField = fieldAccess.value()->getFieldName();

            if (newField == originalField)
            {
                pushableFields.emplace(newField);
            }
        }
    }

    std::vector<LogicalFunction> nonPushable;
    std::vector<LogicalFunction> pushable;

    for (auto&& predicate : predicateSet)
    {
        auto filteredOn = getAccessedFields(predicate);

        const bool isPushable = std::ranges::all_of(
            filteredOn,
            [&pushableFields](const std::string& field)
            {
                // TODO update one schema inference is availabel
                if (pushableFields.contains(field))
                {
                    return true;
                }
                return std::ranges::any_of(
                    pushableFields, [&field](const std::string& pushableField) { return pushableField.ends_with("$" + field); });
            });
        if (isPushable)
        {
            pushable.emplace_back(std::move(predicate));
        }
        else
        {
            nonPushable.emplace_back(std::move(predicate));
        }
    }

    auto newOp = applyToAllChildren(op, pushable);

    return addSelectionIfRequired(newOp, nonPushable);
}

LogicalOperator
PredicatePushdownRule::pushBeyondJoin(TypedLogicalOperator<JoinLogicalOperator> op, std::vector<LogicalFunction> predicateSet) const
{
    PRECONDITION(op.getChildren().size() == 2, "Join operators always must have two children");

    std::vector<LogicalFunction> leftPushable;
    std::vector<LogicalFunction> rightPushable;
    std::vector<LogicalFunction> nonPushable;


    for (auto&& predicate : predicateSet)
    {
        auto filteredOn = getAccessedFields(predicate);

        // TODO use schema inference magic
        const bool allInLeft = std::ranges::all_of(
            filteredOn, [&](const std::string& field) { return op->getLeftSchema().getFieldByName(field).has_value(); });
        const bool allInRight = std::ranges::all_of(
            filteredOn, [&](const std::string& field) { return op->getRightSchema().getFieldByName(field).has_value(); });

        if (allInLeft && !allInRight)
        {
            leftPushable.emplace_back(std::move(predicate));
        }
        else if (allInRight && !allInLeft)
        {
            rightPushable.emplace_back(std::move(predicate));
        }
        else
        {
            nonPushable.emplace_back(std::move(predicate));
        }
    }

    auto left = predicatePushdown(op.getChildren().at(0), leftPushable);
    auto right = predicatePushdown(op.getChildren().at(1), rightPushable);

    auto newOp = op->withChildren({left, right}).withInferredSchema({left.getOutputSchema(), right.getOutputSchema()});

    return addSelectionIfRequired(newOp, nonPushable);
}

LogicalOperator PredicatePushdownRule::pushBeyondIngestionTimeWatermarkAssigner(
    TypedLogicalOperator<IngestionTimeWatermarkAssignerLogicalOperator> op, std::vector<LogicalFunction> predicateSet) const
{
    return applyToAllChildren(std::move(op), std::move(predicateSet));
}

LogicalOperator PredicatePushdownRule::pushBeyondEventTimeWatermarkAssigner(
    TypedLogicalOperator<EventTimeWatermarkAssignerLogicalOperator> op, std::vector<LogicalFunction> predicateSet) const
{
    return applyToAllChildren(std::move(op), std::move(predicateSet));
}

LogicalOperator PredicatePushdownRule::pushBeyondWindowedAggregation(
    TypedLogicalOperator<WindowedAggregationLogicalOperator> op, std::vector<LogicalFunction> predicateSet) const
{
    auto groupingKeys = op->getGroupingKeys();

    std::vector<LogicalFunction> pushable;
    std::vector<LogicalFunction> nonPushable;

    for (auto&& predicate : predicateSet)
    {
        auto accessedFields = getAccessedFields(predicate);
        const bool isPushable = std::ranges::all_of(
            accessedFields,
            [&](const std::string& accessedField)
            {
                return std::ranges::any_of(
                    groupingKeys,
                    [&](const FieldAccessLogicalFunction& groupingKey)
                    {
                        if (groupingKey.getFieldName() == accessedField)
                        {
                            return true;
                        }
                        return groupingKey.getFieldName().ends_with("$" + accessedField);
                    });
            });

        if (isPushable)
        {
            pushable.emplace_back(std::move(predicate));
        }
        else
        {
            nonPushable.emplace_back(std::move(predicate));
        }
    }


    auto newOp = applyToAllChildren(op, std::move(pushable));

    return addSelectionIfRequired(newOp, std::move(nonPushable));
}

std::vector<LogicalFunction> PredicatePushdownRule::splitPredicate(LogicalFunction function) const
{
    if (auto andFunction = function.tryGetAs<AndLogicalFunction>())
    {
        auto children = andFunction.value().getChildren();
        auto splitChildFunctions = splitPredicate(children.at(0));
        auto rightSplitChildFunctions = splitPredicate(children.at(1));

        for (auto&& rightFunction : rightSplitChildFunctions)
        {
            splitChildFunctions.emplace_back(std::move(rightFunction));
        }
        return splitChildFunctions;
    }
    return {std::move(function)};
}

std::vector<std::string> PredicatePushdownRule::getAccessedFields(LogicalFunction logicalFunction) const
{
    return BFSRange(std::move(logicalFunction))
        | std::views::filter([](const LogicalFunction& function) { return function.tryGetAs<FieldAccessLogicalFunction>().has_value(); })
        | std::views::transform([](const LogicalFunction& function)
                                { return function.getAs<FieldAccessLogicalFunction>()->getFieldName(); })
        | std::ranges::to<std::vector>();
}

LogicalOperator PredicatePushdownRule::applyToAllChildren(LogicalOperator op, std::vector<LogicalFunction> predicateSet) const
{
    std::vector<LogicalOperator> children;
    std::vector<Schema> schemas;
    for (const auto& child : op.getChildren())
    {
        auto newChild = predicatePushdown(child, predicateSet);
        schemas.push_back(newChild.getOutputSchema());
        children.push_back(newChild);
    }
    return op.withChildren(children).withInferredSchema(schemas);
}

LogicalOperator PredicatePushdownRule::addSelectionIfRequired(LogicalOperator op, std::vector<LogicalFunction> predicateSet) const
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

    const auto selection = SelectionLogicalOperator{std::move(predicate)};
    return selection.withChildren({op}).withInferredSchema({op.getOutputSchema()});
}

const std::type_info& PredicatePushdownRule::getType()
{
    return typeid(PredicatePushdownRule);
}

std::string_view PredicatePushdownRule::getName()
{
    return NAME;
}

std::set<std::type_index> PredicatePushdownRule::dependsOn() const
{
    return {};
}

std::set<std::type_index> PredicatePushdownRule::requiredBy() const
{
    return {typeid(FixedPlanStructureBarrier)};
}

bool PredicatePushdownRule::operator==(const PredicatePushdownRule& /*other*/) const
{
    return true;
}
}

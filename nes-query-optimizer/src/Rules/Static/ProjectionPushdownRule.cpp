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

#include <Rules/Static/ProjectionPushdownRule.hpp>

#include <algorithm>
#include <array>
#include <set>
#include <string_view>
#include <typeindex>
#include <typeinfo>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Operators/EventTimeWatermarkAssignerLogicalOperator.hpp>
#include <Operators/IngestionTimeWatermarkAssignerLogicalOperator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/UnionLogicalOperator.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Operators/Windows/WindowedAggregationLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Barriers/FixedPlanStructureBarrier.hpp>
#include <Rules/Static/WatermarkAssignerPushdownRule.hpp>
#include <Schema/Field.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{

LogicalOperator projectionPushdown(const LogicalOperator& op, const std::unordered_set<Field>& required);

std::unordered_set<Field> getAccessedFields(const LogicalFunction& logicalFunction)
{
    std::unordered_set<Field> result;
    for (const auto& function : BFSRange(logicalFunction))
    {
        if (function.tryGetAs<FieldAccessLogicalFunction>().has_value())
        {
            result.insert(function.getAs<FieldAccessLogicalFunction>()->getField());
        }
    }
    return result;
}

LogicalOperator pushBeyondSink(const TypedLogicalOperator<SinkLogicalOperator>& op)
{
    /// Use identifiers from output schema as starting point for required fields.
    std::unordered_set<Field> required{};
    for (const auto& field : op->getChild().getOutputSchema())
    {
        required.insert(field);
    }

    auto newChild = projectionPushdown(op->getChild(), required);

    return op.withChildren({newChild}).withInferredSchema();
}

LogicalOperator pushBeyondSource(TypedLogicalOperator<SourceDescriptorLogicalOperator> op, std::unordered_set<Field> required)
{
    /// Recursion stop. Add projection if required is a strict subset of output schema.

    const auto allFieldsAreRequired
        = std::ranges::all_of(op.getOutputSchema(), [&required](const Field& field) { return required.contains(field); });

    if (!allFieldsAreRequired)
    {
        std::vector<ProjectionLogicalOperator::UnboundProjection> projections;
        projections.reserve(required.size());
        for (const auto& field : required)
        {
            projections.emplace_back(field.getLastName(), UnboundFieldAccessLogicalFunction{field.getLastName()});
        }

        return TypedLogicalOperator<ProjectionLogicalOperator>{op, projections, ProjectionLogicalOperator::Asterisk{false}};
    }
    return op;
}

LogicalOperator pushBeyondSelection(const TypedLogicalOperator<SelectionLogicalOperator>& op, const std::unordered_set<Field>& required)
{
    /// Add accessed identifiers in predicate if necessary

    auto newRequired = getAccessedFields(op->getPredicate());
    for (const auto& field : required)
    {
        auto newField = op->getChild().getOutputSchema()[field.getLastName()];
        INVARIANT(newField.has_value(), "all fields in required set must be in child output schema");
        newRequired.insert(newField.value());
    }

    auto child = projectionPushdown(op->getChild(), newRequired);
    return op.withChildren({child}).withInferredSchema();
}

LogicalOperator pushBeyondProjection(const TypedLogicalOperator<ProjectionLogicalOperator>& op, const std::unordered_set<Field>& required)
{
    /// remove unnecessary projections, replace required identifiers via accessed fields from remaining projections

    auto projections = op->getProjections();
    std::unordered_map<Field, LogicalFunction> projectionMap;

    std::vector<ProjectionLogicalOperator::UnboundProjection> newProjections;
    std::unordered_set<Field> newRequired;

    for (const auto& [field, function] : projections)
    {
        projectionMap.emplace(field, function);
    }

    for (const auto& field : required)
    {
        if (auto iter = projectionMap.find(field); iter != projectionMap.end())
        {
            auto function = iter->second;
            for (const auto& accessed : getAccessedFields(function))
            {
                newRequired.insert(accessed);
            }
            newProjections.emplace_back(field.getLastName(), function);
        }
        else
        {
            /// This case is required if the original projection used an asterisk and thus forwarded all child fields.
            auto newField = op->getChild().getOutputSchema()[field.getLastName()];
            INVARIANT(
                newField.has_value(), "required field must be in child output schema, since it is not generated by a projection function");
            newProjections.emplace_back(field.getLastName(), FieldAccessLogicalFunction{newField.value()});
            newRequired.insert(newField.value());
        }
    }

    auto newChild = projectionPushdown(op->getChild(), newRequired);
    return TypedLogicalOperator<ProjectionLogicalOperator>{newChild, newProjections, ProjectionLogicalOperator::Asterisk{false}};
}

LogicalOperator pushBeyondJoin(const TypedLogicalOperator<JoinLogicalOperator>& op, const std::unordered_set<Field>& required)
{
    /// Add accessed identifiers in join predicate if necessary
    /// Apply recursion to both children, each with the relevant subset of required fields that come from the individual child

    auto [left, right] = op->getBothChildren();
    std::unordered_set<Field> requiredLeft;
    std::unordered_set<Field> requiredRight;

    auto predicate = op->getJoinFunction();
    auto joinTimeCharacteristics = op->getJoinTimeCharacteristics();

    INVARIANT(
        (std::holds_alternative<std::array<Windowing::BoundTimeCharacteristic, 2>>(op->getJoinTimeCharacteristics())),
        "join time characteristics should always be bound in this phase.");

    for (const auto& timeCharacteristic : std::get<std::array<Windowing::BoundTimeCharacteristic, 2>>(joinTimeCharacteristics))
    {
        if (std::holds_alternative<Windowing::BoundEventTimeCharacteristic>(timeCharacteristic))
        {
            auto field = std::get<Windowing::BoundEventTimeCharacteristic>(timeCharacteristic).field->getField();
            INVARIANT(field.getProducedBy() == left || field.getProducedBy() == right, "Field must be produced by one of the two children");
            if (field.getProducedBy() == left)
            {
                requiredLeft.insert(field);
            }
            else if (field.getProducedBy() == right)
            {
                requiredRight.insert(field);
            }
        }
    }

    for (const auto& field : getAccessedFields(predicate))
    {
        if (field.getProducedBy() == left)
        {
            requiredLeft.insert(field);
        }
        else if (field.getProducedBy() == right)
        {
            requiredRight.insert(field);
        }
        else
        {
            throw FieldNotFound(fmt::format(
                "the requested join predicate field \"{}\" was not found in either child of the join operator", field.getLastName()));
        }
    }

    for (const auto& field : required)
    {
        auto leftField = left.getOutputSchema()[field.getLastName()];
        auto rightField = right.getOutputSchema()[field.getLastName()];

        if (leftField.has_value())
        {
            requiredLeft.insert(leftField.value());
        }
        else if (rightField.has_value())
        {
            requiredRight.insert(rightField.value());
        }

        /// Start and end fields are generated by join.
        /// Every other field must hbe either comming fromm the left or right child.
        if (!rightField.has_value() && !leftField.has_value() && field.getLastName() != Identifier::parse("start")
            && field.getLastName() != Identifier::parse("end"))
        {
            throw FieldNotFound(
                fmt::format("the requested field \"{}\" was not found in either child of the join operator", field.getLastName()));
        }
    }


    auto newLeft = projectionPushdown(left, requiredLeft);
    auto newRight = projectionPushdown(right, requiredRight);

    return TypedLogicalOperator<JoinLogicalOperator>{
        std::array{newLeft, newRight}, predicate, op->getWindowType(), op->getJoinType(), op->getJoinTimeCharacteristics()};
}

LogicalOperator pushBeyondUnion(const TypedLogicalOperator<UnionLogicalOperator>& op, const std::unordered_set<Field>& required)
{
    /// no new required fields are added in union operator

    std::vector<LogicalOperator> newChildren;
    for (const auto& child : op->getChildren())
    {
        std::unordered_set<Field> newRequired;
        for (const auto& field : required)
        {
            auto newField = child.getOutputSchema()[field.getLastName()];
            INVARIANT(newField.has_value(), "the given field must be available in the plan");
            newRequired.insert(newField.value());
        }
        newChildren.emplace_back(projectionPushdown(child, newRequired));
    }

    return op.withChildren(newChildren).withInferredSchema();
}

LogicalOperator pushBeyondEventTimeWatermarkAssigner(
    const TypedLogicalOperator<EventTimeWatermarkAssignerLogicalOperator>& op, const std::unordered_set<Field>& required)
{
    /// Add eventTime field if necessary

    auto newRequired = getAccessedFields(op->getOnField());
    for (const auto& field : required)
    {
        auto newField = op->getChild().getOutputSchema()[field.getLastName()];
        INVARIANT(newField.has_value(), "the given field must be available in the child operator");
        newRequired.insert(newField.value());
    }

    auto newChild = projectionPushdown(op->getChild(), newRequired);
    return op.withChildren({newChild}).withInferredSchema();
}

LogicalOperator pushBeyondIngestionTimeWatermarkAssigner(
    const TypedLogicalOperator<IngestionTimeWatermarkAssignerLogicalOperator>& op, const std::unordered_set<Field>& required)
{
    /// No new required fields are added in ingestion time watermark assigner operator.

    std::unordered_set<Field> newRequired;
    for (const auto& field : required)
    {
        auto newField = op->getChild().getOutputSchema()[field.getLastName()];
        INVARIANT(newField.has_value(), "the given field must be available in the child operator");
        newRequired.insert(newField.value());
    }

    auto newChild = projectionPushdown(op->getChild(), newRequired);
    return op.withChildren({newChild}).withInferredSchema();
}

LogicalOperator
pushBeyondWindowedAggregation(const TypedLogicalOperator<WindowedAggregationLogicalOperator>& op, const std::unordered_set<Field>& required)
{
    /// Add grouping keys if necessary and remove aggregations that are not accessed later

    std::unordered_set<Field> newRequired;
    std::vector<WindowedAggregationLogicalOperator::ProjectedAggregation> newAggregations;

    std::unordered_set<Identifier> requiredNames;
    for (const auto& field : required)
    {
        requiredNames.insert(field.getLastName());
    }

    /// Iterate over the original aggregations in stable order to preserve output column ordering.
    /// Iterating over `required` (unordered_set) would produce non-deterministic ordering of newAggregations,
    /// causing downstream positional column matching to read wrong aggregation results.
    std::unordered_set<Identifier> aggregationNames;
    for (const auto& agg : op->getWindowAggregation())
    {
        aggregationNames.insert(agg.name);
        if (!requiredNames.contains(agg.name))
        {
            continue;
        }
        newAggregations.push_back(agg);

        INVARIANT(
            std::holds_alternative<TypedLogicalFunction<FieldAccessLogicalFunction>>(agg.function.getInputFunction()),
            "The returned function must always be a bound FieldAccessLogicalFunction");
        const auto fieldAccessFunction = std::get<TypedLogicalFunction<FieldAccessLogicalFunction>>(agg.function.getInputFunction());
        for (const auto& accessedField : getAccessedFields(fieldAccessFunction))
        {
            newRequired.insert(accessedField);
        }
    }

    auto characteristic = std::get<Windowing::BoundTimeCharacteristic>(op->getCharacteristic());
    if (std::holds_alternative<Windowing::BoundEventTimeCharacteristic>(characteristic))
    {
        auto eventTimeCharacteristic = std::get<Windowing::BoundEventTimeCharacteristic>(characteristic);
        newRequired.emplace(eventTimeCharacteristic.field->getField());
    }

    for (const auto& field : required)
    {
        auto isStart = field.getLastName() == Identifier::parse("start");
        auto isEnd = field.getLastName() == Identifier::parse("end");

        if (isStart || isEnd)
        {
            /// If start/end value not available in child, they are generated automatically
            if (auto newField = op->getChild().getOutputSchema()[field.getLastName()])
            {
                newRequired.emplace(newField.value());
            }
        }
        else if (!aggregationNames.contains(field.getLastName()))
        {
            newRequired.emplace(op->getChild(), field.getLastName(), field.getDataType());
        }
    }

    auto groupingKeys = op->getGroupingKeys();
    for (const auto& groupingKey : groupingKeys)
    {
        auto accessedFields = getAccessedFields(groupingKey);
        for (const auto& accessedField : accessedFields)
        {
            newRequired.insert(accessedField);
        }
    }

    auto newChild = projectionPushdown(op->getChild(), newRequired);

    return TypedLogicalOperator<WindowedAggregationLogicalOperator>{
        newChild, op->getGroupingKeysWithName(), newAggregations, op->getWindowType(), op->getCharacteristic()};
}

LogicalOperator pushBeyondDefault(const LogicalOperator& op, const std::unordered_set<Field>& required)
{
    /// Default behavior if concrete operator is not explicitly handled above.
    /// New projection with all required fields is added.
    /// Recursion is restarted for all children with full set of their output schemas.

    std::vector<LogicalOperator> newChildren;

    for (const auto& child : op.getChildren())
    {
        std::unordered_set<Field> newRequired;
        for (const auto& field : child.getOutputSchema())
        {
            newRequired.insert(field);
        }
        newChildren.push_back(projectionPushdown(child, newRequired));
    }

    auto newOp = op.withChildren(newChildren).withInferredSchema();

    std::vector<ProjectionLogicalOperator::UnboundProjection> newProjections;
    for (const auto& field : required)
    {
        auto function = FieldAccessLogicalFunction{Field(newOp, field.getLastName(), field.getDataType())};
        newProjections.emplace_back(field.getLastName(), function);
    }

    return TypedLogicalOperator<ProjectionLogicalOperator>{newOp, newProjections, ProjectionLogicalOperator::Asterisk{false}};
}

LogicalOperator projectionPushdown(const LogicalOperator& op, const std::unordered_set<Field>& required)
{
    if (auto sinkOp = op.tryGetAs<SinkLogicalOperator>())
    {
        return pushBeyondSink(sinkOp.value());
    }
    if (auto sourceOp = op.tryGetAs<SourceDescriptorLogicalOperator>())
    {
        return pushBeyondSource(sourceOp.value(), required);
    }
    if (auto selectionOp = op.tryGetAs<SelectionLogicalOperator>())
    {
        return pushBeyondSelection(selectionOp.value(), required);
    }
    if (auto projectionOp = op.tryGetAs<ProjectionLogicalOperator>())
    {
        return pushBeyondProjection(projectionOp.value(), required);
    }
    if (auto joinOp = op.tryGetAs<JoinLogicalOperator>())
    {
        return pushBeyondJoin(joinOp.value(), required);
    }
    if (auto unionOp = op.tryGetAs<UnionLogicalOperator>())
    {
        return pushBeyondUnion(unionOp.value(), required);
    }
    if (auto eventTimeOp = op.tryGetAs<EventTimeWatermarkAssignerLogicalOperator>())
    {
        return pushBeyondEventTimeWatermarkAssigner(eventTimeOp.value(), required);
    }
    if (auto ingestionTimeOp = op.tryGetAs<IngestionTimeWatermarkAssignerLogicalOperator>())
    {
        return pushBeyondIngestionTimeWatermarkAssigner(ingestionTimeOp.value(), required);
    }
    if (auto windowedAggOp = op.tryGetAs<WindowedAggregationLogicalOperator>())
    {
        return pushBeyondWindowedAggregation(windowedAggOp.value(), required);
    }
    return pushBeyondDefault(op, required);
}
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
LogicalPlan ProjectionPushdownRule::apply(LogicalPlan queryPlan) const
{
    const auto originalRoots = queryPlan.getRootOperators();
    PRECONDITION(originalRoots.size() == 1, "projection pushdown not yet implemented for more than one root");
    auto newRoot = projectionPushdown(originalRoots.at(0), {});
    queryPlan = queryPlan.withRootOperators({newRoot});
    return queryPlan;
}

const std::type_info& ProjectionPushdownRule::getType()
{
    return typeid(ProjectionPushdownRule);
}

std::string_view ProjectionPushdownRule::getName()
{
    return NAME;
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> ProjectionPushdownRule::dependsOn() const
{
    return {typeid(WatermarkAssignerPushdownRule)};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> ProjectionPushdownRule::requiredBy() const
{
    /// Ensures
    return {typeid(FixedPlanStructureBarrier)};
}

bool ProjectionPushdownRule::operator==(const ProjectionPushdownRule&) const
{
    return true;
}


}

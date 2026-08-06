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
#include <ranges>
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
#include <Rules/Barriers/SemanticAnalysisBarrier.hpp>
#include <Rules/PlanVisitor.hpp>
#include <Rules/Static/PredicatePushdownRule.hpp>
#include <Rules/Static/WatermarkAssignerPushdownRule.hpp>
#include <Schema/Field.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <PlanRuleRegistry.hpp>

namespace NES
{
namespace
{


struct OperatorContext
{
    std::variant<
        std::monostate,
        std::vector<ProjectionLogicalOperator::UnboundProjection>,
        std::vector<WindowedAggregationLogicalOperator::ProjectedAggregation>>
        context = std::monostate{};
};

using Visitor = PlanVisitor<OperatorContext, std::unordered_set<Field>>;

Visitor::DownResult projectionPushdown(const LogicalOperator& op, const std::vector<std::unordered_set<Field>>& downContexts);

std::unordered_set<Field> merge(const std::vector<std::unordered_set<Field>>& downContexts)
{
    return downContexts | std::views::join | std::ranges::to<std::unordered_set<Field>>();
}

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

std::vector<Field> sortFields(const std::unordered_set<Field>& fields)
{
    auto sortedFields = std::ranges::to<std::vector>(fields);
    std::ranges::sort(
        sortedFields,
        [](const Field& left, const Field& right)
        { return left.getLastName().asCanonicalString() < right.getLastName().asCanonicalString(); });

    return sortedFields;
}

Visitor::DownResult pushBeyondSink(const TypedLogicalOperator<SinkLogicalOperator>& op)
{
    /// Use identifiers from output schema as starting point for required fields.
    std::unordered_set<Field> required{};
    for (const auto& field : op->getChild().getOutputSchema())
    {
        required.insert(field);
    }

    return {.operatorContext = {}, .downContexts = {{op->getChild(), required}}};
}

Visitor::DownResult
pushBeyondSource(const TypedLogicalOperator<SourceDescriptorLogicalOperator>& op, const std::unordered_set<Field>& required)
{
    /// Recursion stop. Add projection if required is a strict subset of output schema.

    /// Preserve the input schema for constant-only projections.
    if (required.empty())
    {
        return {};
    }

    const auto allFieldsAreRequired
        = std::ranges::all_of(op.getOutputSchema(), [&required](const Field& field) { return required.contains(field); });

    if (!allFieldsAreRequired)
    {
        std::vector<ProjectionLogicalOperator::UnboundProjection> projections;
        projections.reserve(required.size());

        /// Set a fix order in which the projections are applied to ensure deterministic behaviour of rule.
        for (const auto& field : sortFields(required))
        {
            projections.emplace_back(field.getLastName(), UnboundFieldAccessLogicalFunction{field.getLastName()});
        }
        return {.operatorContext{std::move(projections)}, .downContexts = {}};
    }

    return {};
}

Visitor::DownResult pushBeyondSelection(const TypedLogicalOperator<SelectionLogicalOperator>& op, const std::unordered_set<Field>& required)
{
    /// Add accessed identifiers in predicate if necessary

    auto newRequired = getAccessedFields(op->getPredicate());
    for (const auto& field : required)
    {
        auto newField = op->getChild().getOutputSchema()[field.getLastName()];
        INVARIANT(newField.has_value(), "all fields in required set must be in child output schema");
        newRequired.insert(newField.value());
    }

    return {.operatorContext = {}, .downContexts = {{op->getChild(), newRequired}}};
}

Visitor::DownResult
pushBeyondProjection(const TypedLogicalOperator<ProjectionLogicalOperator>& op, const std::unordered_set<Field>& required)
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
    /// Set a fix order in which the projections are applied to ensure deterministic behaviour of rule.
    for (const auto& field : sortFields(required))
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

    return {.operatorContext = {std::move(newProjections)}, .downContexts = {{op->getChild(), newRequired}}};
}

Visitor::DownResult pushBeyondJoin(const TypedLogicalOperator<JoinLogicalOperator>& op, const std::unordered_set<Field>& required)
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

    std::unordered_map<LogicalOperator, std::unordered_set<Field>> downContexts;

    downContexts[left] = std::move(requiredLeft);
    downContexts[right] = std::move(requiredRight);

    return {.operatorContext = {}, .downContexts = std::move(downContexts)};
}

Visitor::DownResult pushBeyondUnion(const TypedLogicalOperator<UnionLogicalOperator>& op, const std::unordered_set<Field>& required)
{
    /// no new required fields are added in union operator

    std::unordered_map<LogicalOperator, std::unordered_set<Field>> downContexts;
    for (const auto& child : op->getChildren())
    {
        std::unordered_set<Field> newRequired;
        for (const auto& field : required)
        {
            auto newField = child.getOutputSchema()[field.getLastName()];
            INVARIANT(newField.has_value(), "the given field must be available in the plan");
            newRequired.insert(newField.value());
        }
        downContexts[child] = newRequired;
    }

    return {.operatorContext = {}, .downContexts = downContexts};
}

Visitor::DownResult pushBeyondEventTimeWatermarkAssigner(
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

    return {.operatorContext = {}, .downContexts = {{op->getChild(), newRequired}}};
}

Visitor::DownResult pushBeyondIngestionTimeWatermarkAssigner(
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

    return {.operatorContext = {}, .downContexts = {{op->getChild(), newRequired}}};
}

Visitor::DownResult
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


    return {.operatorContext = {newAggregations}, .downContexts = {{op->getChild(), newRequired}}};
}

Visitor::DownResult pushBeyondDefault(const LogicalOperator& op, const std::unordered_set<Field>& required)
{
    /// Default behavior if concrete operator is not explicitly handled above.
    /// New projection with all required fields is added.
    /// Recursion is restarted for all children with full set of their output schemas.

    std::unordered_map<LogicalOperator, std::unordered_set<Field>> downContext;

    for (const auto& child : op.getChildren())
    {
        std::unordered_set<Field> newRequired;
        for (const auto& field : child.getOutputSchema())
        {
            newRequired.insert(field);
        }
        downContext[child] = newRequired;
    }


    std::vector<ProjectionLogicalOperator::UnboundProjection> newProjections;
    for (const auto& field : required)
    {
        auto function = UnboundFieldAccessLogicalFunction{field.getLastName()};
        newProjections.emplace_back(field.getLastName(), function);
    }

    return {.operatorContext = {std::move(newProjections)}, .downContexts = std::move(downContext)};
}

Visitor::DownResult projectionPushdown(const LogicalOperator& op, const std::vector<std::unordered_set<Field>>& downContexts)
{
    auto required = merge(downContexts);

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

Visitor::UpResult rebuildPlan(LogicalOperator op, std::vector<LogicalOperator> children, const OperatorContext& context)
{
    if (op.tryGetAs<ProjectionLogicalOperator>())
    {
        PRECONDITION(
            std::holds_alternative<std::vector<ProjectionLogicalOperator::UnboundProjection>>(context.context),
            "OperatorContext must contain UnboundProjections");
        auto projections = std::get<std::vector<ProjectionLogicalOperator::UnboundProjection>>(context.context);

        LogicalOperator newProjection
            = TypedLogicalOperator<ProjectionLogicalOperator>{children.at(0), projections, ProjectionLogicalOperator::Asterisk{false}};
        return newProjection;
    }
    if (const auto windowedAggregationOp = op.tryGetAs<WindowedAggregationLogicalOperator>())
    {
        PRECONDITION(
            std::holds_alternative<std::vector<WindowedAggregationLogicalOperator::ProjectedAggregation>>(context.context),
            "OperatorContext must contain ProjectedAggregations");
        auto aggregations = std::get<std::vector<WindowedAggregationLogicalOperator::ProjectedAggregation>>(context.context);
        const auto& windowedAggregation = windowedAggregationOp.value();
        LogicalOperator newOp = TypedLogicalOperator<WindowedAggregationLogicalOperator>{
            children.at(0),
            windowedAggregation->getGroupingKeysWithName(),
            aggregations,
            windowedAggregation->getWindowType(),
            windowedAggregation->getCharacteristic()};

        return newOp;
    }
    if (op.tryGetAs<SourceDescriptorLogicalOperator>())
    {
        if (!std::holds_alternative<std::vector<ProjectionLogicalOperator::UnboundProjection>>(context.context))
        {
            return op;
        }
        auto projections = std::get<std::vector<ProjectionLogicalOperator::UnboundProjection>>(context.context);
        LogicalOperator newSource
            = TypedLogicalOperator<ProjectionLogicalOperator>{op, projections, ProjectionLogicalOperator::Asterisk{false}};
        return newSource;
    }
    return op.withChildren(children);
}

}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
LogicalPlan ProjectionPushdownRule::apply(LogicalPlan queryPlan) const
{
    Visitor visitor{projectionPushdown, rebuildPlan};

    return visitor.apply(std::move(queryPlan));
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> ProjectionPushdownRule::needs() const
{
    return {typeid(SemanticAnalysisBarrier)};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> ProjectionPushdownRule::wants() const
{
    return {typeid(WatermarkAssignerPushdownRule), typeid(PredicatePushdownRule)};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> ProjectionPushdownRule::neededBy() const
{
    /// Ensures
    return {typeid(FixedPlanStructureBarrier)};
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param)
PlanRuleRegistryReturnType PlanRuleGeneratedRegistrar::RegisterProjectionPushdownPlanRule(PlanRuleRegistryArguments)
{
    return ProjectionPushdownRule{};
}

}

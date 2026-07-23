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
#include <Rules/Static/WatermarkAssignerPushdownRule.hpp>

#include <ranges>
#include <set>
#include <string_view>
#include <typeindex>
#include <typeinfo>
#include <unordered_set>
#include <utility>
#include <vector>

#include <DataTypes/TimeUnit.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/EventTimeWatermarkAssignerLogicalOperator.hpp>
#include <Operators/IngestionTimeWatermarkAssignerLogicalOperator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/UnionLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Barriers/FixedPlanStructureBarrier.hpp>
#include <Rules/Static/PredicatePushdownRule.hpp>
#include <Schema/Field.hpp>
#include <ErrorHandling.hpp>
#include <PlanRewriteUtils.hpp>

namespace NES
{
namespace
{
LogicalOperator watermarkAssignerPushdown(
    PushdownBarrier& ctx, const LogicalOperator& op, bool ingestionTime, std::vector<std::pair<Field, Windowing::TimeUnit>> eventTime);

LogicalOperator
addWatermarkAssigners(LogicalOperator op, const bool ingestionTime, std::vector<std::pair<Field, Windowing::TimeUnit>> eventTime)
{
    /// adds all required watermark assigners

    if (ingestionTime)
    {
        op = TypedLogicalOperator<IngestionTimeWatermarkAssignerLogicalOperator>{op};
    }
    while (!eventTime.empty())
    {
        auto [onField, unit] = eventTime.back();
        auto field = op.getOutputSchema()[onField.getLastName()];
        PRECONDITION(field.has_value(), "the operator must have the event time field");
        eventTime.pop_back();
        op = TypedLogicalOperator<EventTimeWatermarkAssignerLogicalOperator>{op, FieldAccessLogicalFunction{field.value()}, unit};
    }
    return op;
}

std::vector<std::pair<Field, Windowing::TimeUnit>>
fieldsWithNewBase(const LogicalOperator& base, std::vector<std::pair<Field, Windowing::TimeUnit>> eventTime)
{
    for (auto& field : eventTime | std::views::keys)
    {
        auto childField = base.getOutputSchema()[field.getLastName()];
        PRECONDITION(childField.has_value(), "the operator must have the event time field");
        field = childField.value();
    }
    return eventTime;
}

LogicalOperator applyToAllChildren(
    PushdownBarrier& ctx,
    const LogicalOperator& op,
    const bool ingestionTime,
    const std::vector<std::pair<Field, Windowing::TimeUnit>>& eventTime)
{
    std::vector<LogicalOperator> children;
    for (const auto& child : op.getChildren())
    {
        const auto childEventTime = fieldsWithNewBase(child, eventTime);
        auto newChild = watermarkAssignerPushdown(ctx, child, ingestionTime, childEventTime);
        children.push_back(newChild);
    }
    /// withChildren re-infers only the local schema and does not recurse into the children. This relies on the
    /// children's schemas already being inferred, which holds here: the input plan is type-inferred before this
    /// rule runs, and every child was rebuilt through withChildren or a constructor that infers locally.
    /// A recursive withInferredSchema would copy the already-rewritten (potentially shared) subtrees and break
    /// operator sharing in multi-sink plans.
    return op.withChildren(children);
}

LogicalOperator pushBeyondSink(PushdownBarrier& ctx, const TypedLogicalOperator<SinkLogicalOperator>& op)
{
    /// Sinks are the starting point of the recursion.
    /// Because they don't have parents, no watermark assigner
    /// has to be pushed here.
    /// The recursion starts thus with an empty event time watermark assigner set,
    /// and no ingestion time watermark assigner.
    auto newChild = watermarkAssignerPushdown(ctx, op->getChild(), {}, {});
    return op->withChildren({newChild});
}

LogicalOperator pushBeyondSource(
    const TypedLogicalOperator<SourceDescriptorLogicalOperator>& op,
    bool ingestionTime,
    std::vector<std::pair<Field, Windowing::TimeUnit>> eventTime)
{
    return addWatermarkAssigners(op, std::move(ingestionTime), std::move(eventTime));
}

LogicalOperator pushBeyondTransparentOperator(
    PushdownBarrier& ctx,
    const LogicalOperator& op,
    bool ingestionTime,
    const std::vector<std::pair<Field, Windowing::TimeUnit>>& eventTime)
{
    /// Pushes watermark assigners beyond operators that do not affect the
    /// watermark assigners.
    return applyToAllChildren(ctx, op, std::move(ingestionTime), eventTime);
}

LogicalOperator pushBeyondProjection(
    PushdownBarrier& ctx,
    const TypedLogicalOperator<ProjectionLogicalOperator>& op,
    bool ingestionTime,
    const std::vector<std::pair<Field, Windowing::TimeUnit>>& eventTime)
{
    /// test if event time fields are generated by projection.
    /// If not, they are pushed further.
    /// If they are, the event time watermark assigner is applied.

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
            auto newField = field.getLastName();
            auto originalField = fieldAccess.value()->getField().getLastName();

            if (newField == originalField)
            {
                pushableFields.emplace(field);
            }
        }
    }

    std::vector<std::pair<Field, Windowing::TimeUnit>> pushFurther{};
    std::vector<std::pair<Field, Windowing::TimeUnit>> applyNow{};

    for (const auto& [onField, unit] : eventTime)
    {
        auto pushable = pushableFields.contains(onField);
        if (pushable)
        {
            pushFurther.emplace_back(onField, unit);
        }
        else
        {
            applyNow.emplace_back(onField, unit);
        }
    }

    auto newOp = applyToAllChildren(ctx, op, ingestionTime, pushFurther);

    for (const auto& [onField, unit] : std::ranges::reverse_view(applyNow))
    {
        auto field = newOp.getOutputSchema()[onField.getLastName()];
        PRECONDITION(field.has_value(), "the operator must have the event time field");
        newOp = TypedLogicalOperator<EventTimeWatermarkAssignerLogicalOperator>(newOp, FieldAccessLogicalFunction{field.value()}, unit);
    }
    return newOp;
}

LogicalOperator pushBeyondEventTimeWatermarkAssigner(
    PushdownBarrier& ctx,
    const TypedLogicalOperator<EventTimeWatermarkAssignerLogicalOperator>& op,
    const bool ingestionTime,
    std::vector<std::pair<Field, Windowing::TimeUnit>> eventTime)
{
    /// Adds relevant metadata to eventTime variable to push the assigner further down if possible
    /// Does not push event time water mark assigners that don't use a FieldAccessLogicalFunction.

    INVARIANT(
        op->getOnField().tryGetAs<FieldAccessLogicalFunction>().has_value(),
        "EventTime watermark assigner onField must be a FieldAccessLogicalFunction");

    const auto fieldAccess = op->getOnField().tryGetAs<FieldAccessLogicalFunction>();
    eventTime = fieldsWithNewBase(op->getChild(), eventTime);
    eventTime.emplace_back(fieldAccess.value()->getField(), op->getUnit());
    return watermarkAssignerPushdown(ctx, op->getChild(), ingestionTime, eventTime);
}

LogicalOperator pushBeyondIngestionTimeWatermarkAssigner(
    PushdownBarrier& ctx,
    const TypedLogicalOperator<IngestionTimeWatermarkAssignerLogicalOperator>& op,
    bool /* ingestionTime */,
    std::vector<std::pair<Field, Windowing::TimeUnit>> eventTime)
{
    /// Adds flag that a ingestion time watermark assigner is required

    eventTime = fieldsWithNewBase(op->getChild(), eventTime);
    return watermarkAssignerPushdown(ctx, op->getChild(), true, eventTime);
}

LogicalOperator pushBeyondDefault(
    PushdownBarrier& ctx, const LogicalOperator& op, const bool ingestionTime, std::vector<std::pair<Field, Windowing::TimeUnit>> eventTime)
{
    /// Implements default behavior if operator is not explicitly handled.
    /// Applies all watermark assigners and restarts recursion for all children.
    auto newOp = applyToAllChildren(ctx, op, false, {});
    return addWatermarkAssigners(std::move(newOp), ingestionTime, std::move(eventTime));
}

LogicalOperator watermarkAssignerPushdownDispatch(
    PushdownBarrier& ctx, const LogicalOperator& op, bool ingestionTime, std::vector<std::pair<Field, Windowing::TimeUnit>> eventTime)
{
    if (const auto sink = op.tryGetAs<SinkLogicalOperator>())
    {
        return pushBeyondSink(ctx, sink.value());
    }
    if (const auto source = op.tryGetAs<SourceDescriptorLogicalOperator>())
    {
        return pushBeyondSource(source.value(), std::move(ingestionTime), std::move(eventTime));
    }
    if (const auto eventTimeWA = op.tryGetAs<EventTimeWatermarkAssignerLogicalOperator>())
    {
        return pushBeyondEventTimeWatermarkAssigner(ctx, eventTimeWA.value(), std::move(ingestionTime), std::move(eventTime));
    }
    if (const auto ingestionTimeWatermarkAssigner = op.tryGetAs<IngestionTimeWatermarkAssignerLogicalOperator>())
    {
        return pushBeyondIngestionTimeWatermarkAssigner(
            ctx, ingestionTimeWatermarkAssigner.value(), std::move(ingestionTime), std::move(eventTime));
    }
    if (op.tryGetAs<SelectionLogicalOperator>().has_value() || op.tryGetAs<UnionLogicalOperator>().has_value())
    {
        return pushBeyondTransparentOperator(ctx, op, std::move(ingestionTime), eventTime);
    }
    if (const auto projOp = op.tryGetAs<ProjectionLogicalOperator>())
    {
        return pushBeyondProjection(ctx, projOp.value(), std::move(ingestionTime), eventTime);
    }

    return pushBeyondDefault(ctx, op, std::move(ingestionTime), std::move(eventTime));
}

LogicalOperator watermarkAssignerPushdown(
    PushdownBarrier& ctx, const LogicalOperator& op, bool ingestionTime, std::vector<std::pair<Field, Windowing::TimeUnit>> eventTime)
{
    /// A shared operator is a pushdown barrier: pending assigners must not enter a subtree another parent also reads, so
    /// they are materialized above it while the shared subtree itself is rewritten once with no pending assigners.
    if (ctx.isShared(op))
    {
        return ctx.rewriteShared(
            op,
            [&] { return watermarkAssignerPushdownDispatch(ctx, op, false, {}); },
            /// addWatermarkAssigners rebinds the event time fields to the rewritten operator by name
            [&](const LogicalOperator& rewritten) { return addWatermarkAssigners(rewritten, ingestionTime, std::move(eventTime)); });
    }
    return watermarkAssignerPushdownDispatch(ctx, op, std::move(ingestionTime), std::move(eventTime));
}

}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
LogicalPlan WatermarkAssignerPushdownRule::apply(LogicalPlan queryPlan) const
{
    PRECONDITION(not queryPlan.getRootOperators().empty(), "Query must have a sink root operator");

    PushdownBarrier ctx{queryPlan};
    std::vector<LogicalOperator> newRoots;
    newRoots.reserve(queryPlan.getRootOperators().size());
    for (const auto& root : queryPlan.getRootOperators())
    {
        newRoots.push_back(watermarkAssignerPushdown(ctx, root, false, {}));
    }
    return queryPlan.withRootOperators(newRoots);
}

const std::type_info& WatermarkAssignerPushdownRule::getType()
{
    return typeid(WatermarkAssignerPushdownRule);
}

std::string_view WatermarkAssignerPushdownRule::getName()
{
    return NAME;
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> WatermarkAssignerPushdownRule::dependsOn() const
{
    return {typeid(PredicatePushdownRule)};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> WatermarkAssignerPushdownRule::requiredBy() const
{
    return {typeid(FixedPlanStructureBarrier)};
}

bool WatermarkAssignerPushdownRule::operator==(const WatermarkAssignerPushdownRule&) const
{
    return true;
}

}

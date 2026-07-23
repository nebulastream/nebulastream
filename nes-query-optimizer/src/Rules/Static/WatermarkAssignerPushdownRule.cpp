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

#include <DataTypes/TimeUnit.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
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
#include <Rules/Barriers/SemanticAnalysisBarrier.hpp>
#include <Rules/PlanVisitor.hpp>
#include <Rules/Static/PredicatePushdownRule.hpp>
#include <Schema/Field.hpp>
#include <ErrorHandling.hpp>
#include <PlanRuleRegistry.hpp>

namespace NES
{
namespace
{

struct RequiredWatermarkAssigners
{
    bool ingestionTime = false;
    std::vector<std::pair<Field, Windowing::TimeUnit>> eventTime;
};

struct OperatorContext
{
    RequiredWatermarkAssigners toApply;
    std::unordered_map<LogicalOperator, RequiredWatermarkAssigners> pushed;
    bool hasMultipleParents = false;
};

using Visitor = PlanVisitor<OperatorContext, RequiredWatermarkAssigners, bool>;

Visitor::DownResult watermarkAssignerPushdown(const LogicalOperator& op, const std::vector<RequiredWatermarkAssigners>& contexts);

RequiredWatermarkAssigners mergeContexts(const std::vector<RequiredWatermarkAssigners>& contexts)
{
    if (contexts.empty() || contexts.size() > 1)
    {
        return {};
    }
    return contexts.at(0);
}

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

std::unordered_map<LogicalOperator, RequiredWatermarkAssigners>
getChildContexts(const LogicalOperator& op, const RequiredWatermarkAssigners& context)
{
    std::unordered_map<LogicalOperator, RequiredWatermarkAssigners> childContexts;

    for (const auto& child : op.getChildren())
    {
        const RequiredWatermarkAssigners childContext{
            .ingestionTime = context.ingestionTime, .eventTime = fieldsWithNewBase(child, context.eventTime)};
        childContexts.emplace(child, childContext);
    }
    return childContexts;
}

Visitor::DownResult pushBeyondSink(const TypedLogicalOperator<SinkLogicalOperator>& op)
{
    std::unordered_map<LogicalOperator, RequiredWatermarkAssigners> pushed = {{op->getChild(), {.ingestionTime = false, .eventTime = {}}}};
    OperatorContext operatorContext{.toApply = {}, .pushed = pushed};
    return {.operatorContext = std::move(operatorContext), .downContexts = std::move(pushed)};
}

Visitor::DownResult pushBeyondSource(RequiredWatermarkAssigners context)
{
    OperatorContext operatorContext{.toApply = std::move(context), .pushed = {}};
    return {.operatorContext = std::move(operatorContext), .downContexts = {}};
}

Visitor::DownResult pushBeyondTransparentOperator(const LogicalOperator& op, const RequiredWatermarkAssigners& context)
{
    /// Pushes watermark assigners beyond operators that do not affect the
    /// watermark assigners.

    auto pushed = getChildContexts(op, context);
    OperatorContext operatorContext{.toApply = {}, .pushed = pushed};
    return {.operatorContext = std::move(operatorContext), .downContexts = std::move(pushed)};
}

Visitor::DownResult
pushBeyondProjection(const TypedLogicalOperator<ProjectionLogicalOperator>& op, const RequiredWatermarkAssigners& context)
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

    for (const auto& [onField, unit] : context.eventTime)
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

    auto pushed = getChildContexts(op, {.ingestionTime = context.ingestionTime, .eventTime = pushFurther});
    OperatorContext operatorContext{.toApply = {.ingestionTime = false, .eventTime = applyNow}, .pushed = pushed};
    return {.operatorContext = std::move(operatorContext), .downContexts = std::move(pushed)};
}

Visitor::DownResult pushBeyondEventTimeWatermarkAssigner(
    const TypedLogicalOperator<EventTimeWatermarkAssignerLogicalOperator>& op, RequiredWatermarkAssigners context)
{
    /// Adds relevant metadata to eventTime variable to push the assigner further down if possible
    /// Does not push event time water mark assigners that don't use a FieldAccessLogicalFunction.

    INVARIANT(
        op->getOnField().tryGetAs<FieldAccessLogicalFunction>().has_value(),
        "EventTime watermark assigner onField must be a FieldAccessLogicalFunction");

    const auto fieldAccess = op->getOnField().tryGetAs<FieldAccessLogicalFunction>();
    context.eventTime = fieldsWithNewBase(op->getChild(), context.eventTime);

    /// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
    context.eventTime.emplace_back(fieldAccess.value()->getField(), op->getUnit());

    std::unordered_map<LogicalOperator, RequiredWatermarkAssigners> pushed{{op->getChild(), context}};
    OperatorContext operatorContext{.toApply = {}, .pushed = pushed};

    return {.operatorContext = std::move(operatorContext), .downContexts = std::move(pushed)};
}

Visitor::DownResult pushBeyondIngestionTimeWatermarkAssigner(
    const TypedLogicalOperator<IngestionTimeWatermarkAssignerLogicalOperator>& op, RequiredWatermarkAssigners context)
{
    /// Adds flag that a ingestion time watermark assigner is required

    context.eventTime = fieldsWithNewBase(op->getChild(), context.eventTime);

    std::unordered_map<LogicalOperator, RequiredWatermarkAssigners> pushed{
        {op->getChild(), {.ingestionTime = true, .eventTime = context.eventTime}}};
    OperatorContext operatorContext{.toApply = {}, .pushed = pushed};

    return {.operatorContext = std::move(operatorContext), .downContexts = std::move(pushed)};
}

Visitor::DownResult pushBeyondDefault(const LogicalOperator& op, RequiredWatermarkAssigners context)
{
    /// Implements default behavior if operator is not explicitly handled.
    /// Applies all watermark assigners and restarts recursion for all children.

    std::unordered_map<LogicalOperator, RequiredWatermarkAssigners> pushed = getChildContexts(op, {});
    OperatorContext operatorContext{.toApply = std::move(context), .pushed = pushed};

    return {.operatorContext = std::move(operatorContext), .downContexts = std::move(pushed)};
}

Visitor::DownResult watermarkAssignerPushdown(const LogicalOperator& op, const std::vector<RequiredWatermarkAssigners>& downContexts)
{
    const bool hasMultipleParents = downContexts.size() > 1;

    Visitor::DownResult downResult;

    auto context = mergeContexts(downContexts);

    if (const auto sink = op.tryGetAs<SinkLogicalOperator>())
    {
        downResult = pushBeyondSink(sink.value());
    }
    else if (const auto source = op.tryGetAs<SourceDescriptorLogicalOperator>())
    {
        downResult = pushBeyondSource(context);
    }
    else if (const auto eventTimeWA = op.tryGetAs<EventTimeWatermarkAssignerLogicalOperator>())
    {
        downResult = pushBeyondEventTimeWatermarkAssigner(eventTimeWA.value(), std::move(context));
    }
    else if (const auto ingestionTimeWatermarkAssigner = op.tryGetAs<IngestionTimeWatermarkAssignerLogicalOperator>())
    {
        downResult = pushBeyondIngestionTimeWatermarkAssigner(ingestionTimeWatermarkAssigner.value(), std::move(context));
    }
    else if (op.tryGetAs<SelectionLogicalOperator>().has_value() || op.tryGetAs<UnionLogicalOperator>().has_value())
    {
        downResult = pushBeyondTransparentOperator(op, context);
    }
    else if (const auto projOp = op.tryGetAs<ProjectionLogicalOperator>())
    {
        downResult = pushBeyondProjection(projOp.value(), context);
    }
    else
    {
        downResult = pushBeyondDefault(op, std::move(context));
    }

    downResult.operatorContext.hasMultipleParents = hasMultipleParents;

    return downResult;
}

Visitor::UpResult rebuildPlan(
    LogicalOperator op,
    std::vector<LogicalOperator> children,
    OperatorContext context,
    const std::unordered_map<LogicalOperator, bool>& childWithMultipleParents)
{
    const auto originalChildren = op.getChildren();
    for (size_t i = 0; i < children.size(); ++i)
    {
        if (childWithMultipleParents.at(children[i]) && context.pushed.contains(originalChildren[i]))
        {
            auto [ingestionTime, eventTime] = context.pushed.at(originalChildren.at(i));
            children[i] = addWatermarkAssigners(children[i], ingestionTime, std::move(eventTime));
        }
    }

    if (op.tryGetAs<IngestionTimeWatermarkAssignerLogicalOperator>().has_value()
        || op.tryGetAs<EventTimeWatermarkAssignerLogicalOperator>().has_value())
    {
        PRECONDITION(children.size() == 1, "WatermarkAssigners can only have one child");
        return {children[0], {}};
    }

    op = op.withChildren(children);
    return {addWatermarkAssigners(op, context.toApply.ingestionTime, std::move(context.toApply.eventTime)), context.hasMultipleParents};
}

}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
LogicalPlan WatermarkAssignerPushdownRule::apply(LogicalPlan queryPlan) const
{
    Visitor visitor{watermarkAssignerPushdown, rebuildPlan};
    return visitor.apply(std::move(queryPlan));
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> WatermarkAssignerPushdownRule::needs() const
{
    return {typeid(SemanticAnalysisBarrier)};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> WatermarkAssignerPushdownRule::wants() const
{
    return {typeid(PredicatePushdownRule)};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> WatermarkAssignerPushdownRule::neededBy() const
{
    return {typeid(FixedPlanStructureBarrier)};
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param)
PlanRuleRegistryReturnType WatermarkAssignerPushdownRule::create(PlanRuleRegistryArguments)
{
    return WatermarkAssignerPushdownRule{};
}

}

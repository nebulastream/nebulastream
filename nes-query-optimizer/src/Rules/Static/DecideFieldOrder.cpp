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

#include <Rules/Static/DecideFieldOrder.hpp>

#include <algorithm>
#include <memory>
#include <ranges>
#include <set>
#include <string_view>
#include <typeindex>
#include <typeinfo>
#include <unordered_map>
#include <utility>
#include <vector>

#include <DataTypes/Schema.hpp>
#include <DataTypes/SchemaFwd.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/Reorderer.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Barriers/FixedPlanStructureBarrier.hpp>
#include <Rules/PlanVisitor.hpp>
#include <Rules/Static/DecideFieldMappings.hpp>
#include <Rules/Static/DecideJoinTypesRule.hpp>
#include <Rules/Static/DecideMemoryLayoutRule.hpp>
#include <Rules/Static/OriginIdInferenceRule.hpp>
#include <Schema/Binder.hpp>
#include <Schema/Field.hpp>
#include <Traits/FieldOrderingTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/Variant.hpp>
#include <ErrorHandling.hpp>
#include <PlanRuleRegistry.hpp>

namespace NES
{

/// Currently, we use the same algorithm that CalcTargetOrderRule uses, to determine which field order to use at each operator.
/// In the future, this might be something we want to optimize over more aggressively.
/// Analogue to CalcTargetOrderRule:
/// Calculates the schema of anonymous sinks if no schema was specified deterministically.
/// The default behavior for every operator is the following:
/// 1. For any child (in the order of the children): For any of its output fields (In the output order determined for the child):
///     If the current operator outputs a fields that has the same name, append it to the list of fields
/// 2. Any field that does not appear in the input to the operator, is appended to the ordered output fields in lexicographic order.
///
/// Concrete operators can overwrite this behavior by overwriting Reorderer.
///
/// Implemented as a two-pass PlanVisitor over the (possibly DAG-shaped) plan: down-pass propagates the sink's required
/// order to its child, up-pass rebuilds bottom-up assigning each op a FieldOrderingTrait (propagated, or computed via
/// calculateOutputOrder). Shared subplans (multi-sink) get a synthetic Projection per sink to hold sink-specific ordering,
/// see setSinkFieldOrder.
namespace
{

struct OperatorContext
{
    Schema<UnqualifiedUnboundField, Ordered> requiredFieldOrdering;

    /// True if this op has >1 parent in the plan DAG (shared subplan). A shared op can only hold one FieldOrderingTrait,
    /// so setSinkFieldOrder must not impose its sink's order directly on it; instead it wraps it in a per-sink projection.
    bool hasMultipleParents = false;
};

using Visitor = PlanVisitor<OperatorContext, Schema<UnqualifiedUnboundField, Ordered>, bool>;

Visitor::UpResult setSinkFieldOrder(
    const LogicalOperator& op, std::vector<LogicalOperator> children, const OperatorContext& operatorContext, bool childHasMultipleParents)
{
    PRECONDITION(op.tryGetAs<SinkLogicalOperator>(), "function can only be executed on SinkLogicalOperator");

    auto sink = op.getAs<SinkLogicalOperator>();

    PRECONDITION(children.size() == 1, "Sinks can only have exactly one child");
    auto child = children.at(0);

    auto sinkDescriptorOpt = sink->getSinkDescriptor();
    PRECONDITION(sinkDescriptorOpt.has_value(), "Root sink must have a sink descriptor");
    auto targetSchema = *NES::get<std::shared_ptr<const Schema<UnqualifiedUnboundField, Ordered>>>(sinkDescriptorOpt->getSchema());
    for (const auto& targetField : targetSchema)
    {
        PRECONDITION(
            children.at(0).getOutputSchema().contains(targetField.getFullyQualifiedName()),
            "Field {} not present in root child output schema",
            targetField.getFullyQualifiedName());
    }

    if (childHasMultipleParents)
    {
        auto newChild = ProjectionLogicalOperator::create(child, {}, ProjectionLogicalOperator::Asterisk{true});
        auto traitSet = newChild.getTraitSet();
        traitSet.insert(FieldOrderingTrait{operatorContext.requiredFieldOrdering});
        child = newChild.withChildren({child}).withTraitSet(traitSet);
    }

    auto rootTraitSet = sink->getTraitSet();
    rootTraitSet.insert(FieldOrderingTrait{Schema<UnqualifiedUnboundField, Ordered>{}});

    return {op.withChildren({child}).withTraitSet(rootTraitSet), false};
}

Schema<Field, Ordered> calculateOutputOrder(const LogicalOperator& op, std::vector<LogicalOperator> children)
{
    const std::unordered_map<TypedLogicalOperator<>, Schema<Field, Ordered>> childrenWithOutputOrder
        = children
        | std::views::transform(
              [](const auto& child) -> std::pair<TypedLogicalOperator<>, Schema<Field, Ordered>>
              {
                  return std::pair{
                      child,
                      child->getTraitSet().template get<FieldOrderingTrait>()->getOrderedFields() | RangeBinder{child}
                          | std::ranges::to<Schema<Field, Ordered>>()};
              })
        | std::ranges::to<std::unordered_map>();

    if (const auto reorderer = op.tryGetAs<Reorderer>())
    {
        return reorderer.value()->get().getOrderedOutputSchema([&childrenWithOutputOrder](const LogicalOperator& child)
                                                               { return childrenWithOutputOrder.at(child); });
    }

    std::vector<Field> newOutputOrder;
    std::vector<Field> rest;
    const auto outputSchema = op.getOutputSchema();
    const auto orderedBoundInputSchema = children
        | std::views::transform([&](const auto& child) { return childrenWithOutputOrder.at(child); }) | std::views::join
        | std::ranges::to<Schema<Field, Ordered>>();
    for (const auto& inputField : orderedBoundInputSchema)
    {
        if (const auto& outputFieldOpt = outputSchema[inputField.getFullyQualifiedName()])
        {
            newOutputOrder.push_back(outputFieldOpt.value());
        }
    }

    for (const auto& outputField : outputSchema)
    {
        if (!orderedBoundInputSchema.contains(outputField.getFullyQualifiedName()))
        {
            rest.push_back(outputField);
        }
    }

    std::ranges::sort(
        rest,
        [](const auto& lhs, const auto& rhs)
        { return fmt::format("{}", lhs.getFullyQualifiedName()) < fmt::format("{}", rhs.getFullyQualifiedName()); });

    for (const auto& field : rest)
    {
        newOutputOrder.push_back(field);
    }
    return newOutputOrder | std::ranges::to<Schema<Field, Ordered>>();
}

Visitor::UpResult decideFieldOrder(
    LogicalOperator op,
    const std::vector<LogicalOperator>& children,
    const OperatorContext& operatorContext,
    const std::unordered_map<LogicalOperator, bool>& childHasMultipleParents)
{
    /// For now we reuse the semantic field order of the operators as a heuristic for deciding the FieldOrdering trait
    /// Other strategies may be explored for better physical optimization

    if (auto sinkOp = op.tryGetAs<SinkLogicalOperator>())
    {
        PRECONDITION(children.size() == 1, "A sink can only have one child");
        return setSinkFieldOrder(op, children, operatorContext, childHasMultipleParents.at(children.at(0)));
    }
    op = op.withChildren(children);

    auto traitSet = op.getTraitSet();

    if (operatorContext.requiredFieldOrdering.size() > 0)
    {
        traitSet.insert(FieldOrderingTrait{unbind(operatorContext.requiredFieldOrdering)});
    }
    else
    {
        const auto outputOrder = calculateOutputOrder(op, children);
        traitSet.insert(FieldOrderingTrait{unbind(outputOrder)});
    }

    return {op.withTraitSet(std::move(traitSet)), operatorContext.hasMultipleParents};
}

}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> DecideFieldOrder::needs() const
{
    return {typeid(FixedPlanStructureBarrier)};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> DecideFieldOrder::wantedBy() const
{
    return {typeid(DecideFieldMappings), typeid(DecideJoinTypesRule), typeid(DecideMemoryLayoutRule), typeid(OriginIdInferenceRule)};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
LogicalPlan DecideFieldOrder::apply(const LogicalPlan& queryPlan) const
{
    Visitor visitor{
        [](const LogicalOperator& op, const std::vector<Schema<UnqualifiedUnboundField, Ordered>>& downContext) -> Visitor::DownResult
        {
            if (auto sinkOp = op.tryGetAs<SinkLogicalOperator>())
            {
                const auto& sink = sinkOp.value();

                auto sinkDescriptorOpt = sink->getSinkDescriptor();
                PRECONDITION(sinkDescriptorOpt.has_value(), "Root sink must have a sink descriptor");
                auto targetSchema
                    = *NES::get<std::shared_ptr<const Schema<UnqualifiedUnboundField, Ordered>>>(sinkDescriptorOpt->getSchema());

                const OperatorContext context = {.requiredFieldOrdering = targetSchema, .hasMultipleParents = downContext.size() > 1};
                return {.operatorContext = context, .downContexts = {{sink->getChild(), targetSchema}}};
            }
            if (downContext.size() == 1)
            {
                return {
                    .operatorContext = {.requiredFieldOrdering = downContext.at(0), .hasMultipleParents = downContext.size() > 1},
                    .downContexts = {}};
            }
            return {.operatorContext = {.requiredFieldOrdering = {}, .hasMultipleParents = downContext.size() > 1}, .downContexts = {}};
        },
        decideFieldOrder};

    return visitor.apply(queryPlan);
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param)
PlanRuleRegistryReturnType PlanRuleGeneratedRegistrar::RegisterDecideFieldOrderPlanRule(PlanRuleRegistryArguments)
{
    return DecideFieldOrder{};
}
}

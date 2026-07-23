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
#include <Rules/Semantic/CalcTargetOrderRule.hpp>

#include <algorithm>
#include <memory>
#include <ranges>
#include <set>
#include <string_view>
#include <typeindex>
#include <typeinfo>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include <DataTypes/UnboundField.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/Reorderer.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Barriers/SemanticAnalysisBarrier.hpp>
#include <Rules/PlanVisitor.hpp>
#include <Rules/Semantic/AnonymousSinkBindingRule.hpp>
#include <Rules/Semantic/LogicalSourceExpansionRule.hpp>
#include <Rules/Semantic/SinkBindingRule.hpp>
#include <Rules/Semantic/TypeInferenceRule.hpp>
#include <Schema/Binder.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Overloaded.hpp>
#include <Util/Variant.hpp>
#include <ErrorHandling.hpp>
#include <PlanRuleRegistry.hpp>

namespace NES
{

namespace
{
bool hasOrder(const LogicalOperator& rootNode)
{
    const auto sink = rootNode.getAs<SinkLogicalOperator>();
    PRECONDITION(sink->getSinkDescriptor().has_value(), "Expected all sink descriptors to be set");
    /// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
    const auto schemaVariant = sink->getSinkDescriptor()->getSchema();
    return std::visit(
        Overloaded{
            [](const std::monostate&) -> bool
            {
                PRECONDITION(false, "Expected schema to be set in sink descriptor, was schema inference run?");
                std::unreachable();
            },
            [](const std::shared_ptr<const Schema<UnqualifiedUnboundField, Unordered>>&) { return false; },
            [](const std::shared_ptr<const Schema<UnqualifiedUnboundField, Ordered>>&) { return true; }},
        schemaVariant);
}
}

CalcTargetOrderRule::Visitor::UpResult CalcTargetOrderRule::calcTargetOrder(
    LogicalOperator op, std::vector<LogicalOperator> children, std::unordered_map<LogicalOperator, Schema<Field, Ordered>> upContexts)
{
    op = op.withChildren(children);

    if (auto sinkOp = op.tryGetAs<SinkLogicalOperator>())
    {
        const auto& sink = sinkOp.value();

        /// avoid overwrite sink orders if they are predefined
        if (hasOrder(op))
        {
            return {op, {}};
        }

        PRECONDITION(children.size() == 1, "Sinks can only have one child");

        const auto newTargetSchema
            = upContexts.at(children.at(0)) | RangeUnbinder{} | std::ranges::to<Schema<UnqualifiedUnboundField, Ordered>>();
        auto sinkDescriptorOpt = sink->getSinkDescriptor();
        PRECONDITION(sinkDescriptorOpt.has_value(), "Sink operator must have a descriptor to infer target schema order");
        const auto oldDescriptor = NES::get<AnonymousSinkDescriptor>(sinkDescriptorOpt->getUnderlying());
        auto newAnonymousSinkDescriptor = SinkDescriptor{oldDescriptor.withSchemaOrder(newTargetSchema)};
        auto newSinkRoot = sink->withSinkDescriptor(newAnonymousSinkDescriptor);

        return {newSinkRoot, {}};
    }

    if (auto reorderer = op.tryGetAs<Reorderer>())
    {
        auto schema
            = reorderer.value()->get().getOrderedOutputSchema([&upContexts](const LogicalOperator& child) { return upContexts.at(child); });
        return {op, schema};
    }

    /// the unordered map above does not maintain the order of the children, so we have to go through them again
    const auto orderedBoundInputSchema = children | std::views::transform([&](const auto& child) { return upContexts.at(child); })
        | std::views::join | std::ranges::to<Schema<Field, Ordered>>();

    std::vector<Field> outputOrder;
    std::vector<Field> rest;
    const auto outputSchema = op.getOutputSchema();
    for (const auto& inputField : orderedBoundInputSchema)
    {
        if (const auto& outputFieldOpt = outputSchema[inputField.getFullyQualifiedName()])
        {
            outputOrder.push_back(outputFieldOpt.value());
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
        outputOrder.push_back(field);
    }

    auto schema = std::move(outputOrder) | std::ranges::to<Schema<Field, Ordered>>();

    return {op, schema};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
LogicalPlan CalcTargetOrderRule::apply(NES::LogicalPlan plan) const
{
    if (std::ranges::all_of(plan.getRootOperators(), hasOrder))
    {
        return plan;
    }

    Visitor visitor{[](const LogicalOperator& op,
                       std::vector<LogicalOperator> children,
                       std::unordered_map<LogicalOperator, Schema<Field, Ordered>> upContexts)
                    { return calcTargetOrder(op, std::move(children), std::move(upContexts)); }};

    return visitor.apply(plan);
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> CalcTargetOrderRule::needs() const
{
    return {typeid(SinkBindingRule), typeid(AnonymousSinkBindingRule), typeid(LogicalSourceExpansionRule), typeid(TypeInferenceRule)};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> CalcTargetOrderRule::neededBy() const
{
    return {typeid(SemanticAnalysisBarrier)};
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param)
PlanRuleRegistryReturnType CalcTargetOrderRule::create(PlanRuleRegistryArguments)
{
    return CalcTargetOrderRule{};
}
};

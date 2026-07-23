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

#include <DataTypes/Schema.hpp>
#include <DataTypes/SchemaFwd.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/Reorderer.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Semantic/AnonymousSinkBindingRule.hpp>
#include <Rules/Semantic/LogicalSourceExpansionRule.hpp>
#include <Rules/Semantic/SinkBindingRule.hpp>
#include <Rules/Semantic/TypeInferenceRule.hpp>
#include <Schema/Binder.hpp>
#include <Schema/Field.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Overloaded.hpp>
#include <Util/Variant.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{
Schema<Field, Ordered> applyRecursive(const LogicalOperator& visiting)
{
    const auto childrenWithOutputOrder = visiting.getChildren()
        | std::views::transform([](const auto& child) { return std::pair{child, applyRecursive(child)}; })
        | std::ranges::to<std::unordered_map>();


    if (const auto reorderer = visiting.tryGetAs<Reorderer>())
    {
        return reorderer.value()->get().getOrderedOutputSchema([&childrenWithOutputOrder](const LogicalOperator& child)
                                                               { return childrenWithOutputOrder.at(child); });
    }
    /// the unordered map above does not maintain the order of the children, so we have to go through them again
    const auto orderedBoundInputSchema = visiting->getChildren()
        | std::views::transform([&](const auto& child) { return childrenWithOutputOrder.at(child); }) | std::views::join
        | std::ranges::to<Schema<Field, Ordered>>();

    std::vector<Field> outputOrder;
    std::vector<Field> rest;
    const auto outputSchema = visiting.getOutputSchema();
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
    return outputOrder | std::ranges::to<Schema<Field, Ordered>>();
}

}

const std::type_info& CalcTargetOrderRule::getType()
{
    return typeid(CalcTargetOrderRule);
}

std::string_view CalcTargetOrderRule::getName()
{
    return NAME;
};

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> CalcTargetOrderRule::dependsOn() const
{
    return {typeid(SinkBindingRule), typeid(AnonymousSinkBindingRule), typeid(LogicalSourceExpansionRule), typeid(TypeInferenceRule)};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> CalcTargetOrderRule::requiredBy() const
{
    return {};
}

bool CalcTargetOrderRule::operator==(const CalcTargetOrderRule&) const
{
    return true;
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
LogicalPlan CalcTargetOrderRule::apply(NES::LogicalPlan plan) const
{
    auto hasOrder = [](const LogicalOperator& rootNode)
    {
        const auto sink = rootNode.getAs<SinkLogicalOperator>();
        PRECONDITION(sink->getSinkDescriptor().has_value(), "Expected all sink descriptors to be set");
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
    };

    if (std::ranges::all_of(plan.getRootOperators(), hasOrder))
    {
        return plan;
    }

    /// Infer the target schema order for every sink that does not have one yet. The traversal below is read-only,
    /// so operators shared between multiple sinks stay shared.
    std::vector<LogicalOperator> newRoots;
    newRoots.reserve(plan.getRootOperators().size());
    for (const auto& rootOperator : plan.getRootOperators())
    {
        if (hasOrder(rootOperator))
        {
            newRoots.push_back(rootOperator);
            continue;
        }
        const auto root = rootOperator.getAs<SinkLogicalOperator>();
        auto outputOrder = applyRecursive(root->getChild());
        const auto newTargetSchema
            = outputOrder | std::views::transform(Unbinder<Field>{}) | std::ranges::to<Schema<UnqualifiedUnboundField, Ordered>>();
        auto sinkDescriptorOpt = root->getSinkDescriptor();
        PRECONDITION(sinkDescriptorOpt.has_value(), "Sink operator must have a descriptor to infer target schema order");
        const auto oldDescriptor = NES::get<AnonymousSinkDescriptor>(sinkDescriptorOpt->getUnderlying());
        auto newAnonymousSinkDescriptor = SinkDescriptor{oldDescriptor.withSchemaOrder(newTargetSchema)};
        newRoots.push_back(root->withSinkDescriptor(newAnonymousSinkDescriptor));
    }
    return plan.withRootOperators(newRoots);
}
};

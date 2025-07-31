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
#include <LegacyOptimizer/CalcTargetOrderPhase.hpp>

#include "Operators/Reorderer.hpp"
#include "Operators/Sinks/SinkLogicalOperator.hpp"
#include "Schema/Field.hpp"
#include "Sinks/SinkDescriptor.hpp"
#include "Util/Overloaded.hpp"

namespace NES
{

namespace
{
Schema<Field, Ordered> applyRecursive(const LogicalOperator& visiting)
{
    const auto childrenWithOutputOrder = visiting.getChildren()
        | std::views::transform([](const auto& child)
                                { return std::pair{child, applyRecursive(child)}; })
        | std::ranges::to<std::unordered_map>();

    /// the unordered map above does not maintain the order of the children, so we have to go through them again
    const auto orderedBoundInputSchema = visiting->getChildren()
        | std::views::transform([&](const auto& child) { return childrenWithOutputOrder.at(child); }) | std::views::join
        | std::ranges::to<Schema<Field, Ordered>>();

    if (const auto reorderer = visiting.tryGetAs<Reorderer>())
    {
        return reorderer.value()->get().getOrderedOutputSchema([&childrenWithOutputOrder](const LogicalOperator& child)
                                                               { return childrenWithOutputOrder.at(child); });
    }
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

void CalcTargetOrderPhase::apply(NES::LogicalPlan& plan) const
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
        return;
    }

    PRECONDITION(std::ranges::size(plan.getRootOperators()) == 1, "Can only infer target schema order for plans with exactly one root");
    const auto root = plan.getRootOperators()[0].getAs<SinkLogicalOperator>();
    auto outputOrder = applyRecursive(root->getChild());
    const auto newTargetSchema
        = outputOrder | std::views::transform(Unbinder<Field>{}) | std::ranges::to<Schema<UnqualifiedUnboundField, Ordered>>();
    const auto oldDescriptor = std::get<InlineSinkDescriptor>(root->getSinkDescriptor()->getUnderlying());
    auto newInlineSinkDescriptor = SinkDescriptor{oldDescriptor.withSchemaOrder(newTargetSchema)};
    auto newSinkRoot = root->withSinkDescriptor(std::move(newInlineSinkDescriptor));
    plan = plan.withRootOperators({newSinkRoot});
}
};
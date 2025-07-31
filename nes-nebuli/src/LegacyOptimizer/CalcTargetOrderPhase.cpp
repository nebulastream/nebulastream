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

#include "Operators/Sinks/SinkLogicalOperator.hpp"
#include "Schema/Field.hpp"
#include "Sinks/SinkDescriptor.hpp"
#include "Util/Overloaded.hpp"

namespace NES
{

namespace
{
std::pair<LogicalOperator, std::vector<Field>> applyRecursive(const LogicalOperator& visiting)
{
    const auto childrenWithOutputOrder = visiting.getChildren()
        | std::views::transform([](const auto& child) { return applyRecursive(child); }) | std::ranges::to<std::unordered_map>();

    const auto orderedBoundInputSchema
        = std::views::values(childrenWithOutputOrder) | std::views::join | std::ranges::to<SchemaBase<Field, true>>();

    std::vector<Field> outputOrder;
    std::vector<Field> rest;
    for (const auto& outputField : visiting->getOutputSchema())
    {
        if (orderedBoundInputSchema.contains(outputField.getFullyQualifiedName()))
        {
            outputOrder.push_back(outputField);
        }
        else
        {
            rest.push_back(outputField);
        }
    }

    /// For now alphanumeric ordering for all the new fields, we might want to make this controllable by the operators via a type trait
    std::ranges::sort(
        rest,
        [](const auto& lhs, const auto& rhs)
        { return fmt::format("{}", lhs.getFullyQualifiedName()) < fmt::format("{}", rhs.getFullyQualifiedName()); });
    for (const auto& field : rest)
    {
        outputOrder.push_back(field);
    }
    return {visiting, std::move(outputOrder)};
}

}

void CalcTargetOrderPhase::apply(NES::LogicalPlan& plan)
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
                [](const std::shared_ptr<const SchemaBase<UnboundFieldBase<1>, false>>&) { return false; },
                [](const std::shared_ptr<const SchemaBase<UnboundFieldBase<1>, true>>&) { return true; }},
            schemaVariant);
    };

    if (std::ranges::all_of(plan.getRootOperators(), hasOrder))
    {
        return;
    }

    PRECONDITION(std::ranges::size(plan.getRootOperators()) == 1, "Can only infer target schema order for plans with exactly one root");
    const auto root = plan.getRootOperators()[0].getAs<SinkLogicalOperator>();
    auto [newRoot, outputOrder] = applyRecursive(root);
    const auto newTargetSchema
        = outputOrder | std::views::transform(Field::unbinder()) | std::ranges::to<SchemaBase<UnboundFieldBase<1>, true>>();
    const auto& oldDescriptor = std::get<InlineSinkDescriptor>(root->getSinkDescriptor()->getUnderlying());
    auto newSinkDescriptor = SinkDescriptor{InlineSinkDescriptor{oldDescriptor.withSchemaOrder(newTargetSchema)}};
    auto newSinkRoot = newRoot.getAs<SinkLogicalOperator>()->withSinkDescriptor(std::move(newSinkDescriptor));
    plan = plan.withRootOperators({newSinkRoot});
}
};
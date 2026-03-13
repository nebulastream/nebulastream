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

#include <Phases/DecideFieldOrder.hpp>
#include "Operators/ProjectionLogicalOperator.hpp"
#include "Operators/Sinks/SinkLogicalOperator.hpp"
#include "Operators/Sources/SourceDescriptorLogicalOperator.hpp"
#include "Traits/FieldOrderingTrait.hpp"
#include "Traits/TraitSet.hpp"

namespace NES
{
namespace
{
LogicalOperator applyRecur(const LogicalOperator& visiting)
{
    /// For now we reuse the semantic field order of the operators as a heuristic for deciding the FieldOrdering trait
    /// Other strategies may be explored for better physical optimization
    const auto oldWithNewChildren = visiting.getChildren()
        | std::views::transform([&](const auto& child) { return std::pair{child, applyRecur(child)}; }) | std::ranges::to<std::vector>();
    const auto childrenMap = oldWithNewChildren | std::ranges::to<std::unordered_map>();
    const auto newChildren
        = oldWithNewChildren | std::views::transform(&std::pair<LogicalOperator, LogicalOperator>::second) | std::ranges::to<std::vector>();

    const auto childrenWithOutputOrder
        = newChildren
        | std::views::transform(
              [](const auto& child)
              { return std::pair{child, bind(child, child->getTraitSet().template get<FieldOrderingTrait>()->getOrderedFields())}; })
        | std::ranges::to<std::unordered_map>();


    Schema<Field, Ordered> outputOrder = [&]
    {
        if (const auto reorderer = visiting.tryGetAs<Reorderer>())
        {
            return reorderer.value()->get().getOrderedOutputSchema([&childrenWithOutputOrder, &childrenMap](const LogicalOperator& child)
                                                                   { return childrenWithOutputOrder.at(childrenMap.at(child)); });
        }
        std::vector<Field> outputOrder;
        std::vector<Field> rest;
        const auto outputSchema = visiting.getOutputSchema();
        const auto orderedBoundInputSchema = newChildren
            | std::views::transform([&](const auto& child) { return childrenWithOutputOrder.at(child); }) | std::views::join
            | std::ranges::to<Schema<Field, Ordered>>();
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
    }();

    auto traitSet = visiting.getTraitSet();
    traitSet.insert(FieldOrderingTrait{unbind(outputOrder)});
    return visiting.withTraitSet(std::move(traitSet)).withChildren(std::move(newChildren));
}

}

LogicalPlan DecideFieldOrder::apply(const LogicalPlan& queryPlan)
{
    PRECONDITION(
        std::ranges::size(queryPlan.getRootOperators()) == 1,
        "query plan must have exactly one root operator but has {}",
        std::ranges::size(queryPlan.getRootOperators()));

    auto rootSink = queryPlan.getRootOperators()[0].getAs<SinkLogicalOperator>();
    auto oldRootChild = rootSink->getChild();

    const auto newRootChild = [&]
    {
        if (std::ranges::size(oldRootChild->getChildren()) > 0)
        {
            auto newGrandchildren = oldRootChild->getChildren()
                | std::views::transform([](const auto& grandchild) { return applyRecur(grandchild); }) | std::ranges::to<std::vector>();
            return oldRootChild.withChildren(std::move(newGrandchildren));
        }
        return oldRootChild;
    }();

    auto targetSchema
        = *std::get<std::shared_ptr<const Schema<UnqualifiedUnboundField, Ordered>>>(rootSink->getSinkDescriptor()->getSchema());
    for (const auto& targetField : targetSchema)
    {
        PRECONDITION(
            newRootChild.getOutputSchema().contains(targetField.getFullyQualifiedName()),
            "Field {} not present in root child output schema",
            targetField.getFullyQualifiedName());
    }
    TraitSet rootChildTraitSet = newRootChild.getTraitSet();
    rootChildTraitSet.insert(FieldOrderingTrait{targetSchema});
    const auto rootChildWithTargetOrder = newRootChild.withTraitSet(TraitSet{rootChildTraitSet});

    auto rootTraitSet = rootSink->getTraitSet();
    rootTraitSet.insert(FieldOrderingTrait{Schema<UnqualifiedUnboundField, Ordered>{}});

    auto newRoot = rootSink.withChildren({rootChildWithTargetOrder}).withTraitSet(rootTraitSet);
    return queryPlan.withRootOperators({std::move(newRoot)});
}
}

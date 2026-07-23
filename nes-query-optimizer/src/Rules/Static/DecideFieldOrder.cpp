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
#include <cstddef>
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
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/Reorderer.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Barriers/FixedPlanStructureBarrier.hpp>
#include <Schema/Binder.hpp>
#include <Schema/Field.hpp>
#include <Traits/FieldOrderingTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/Variant.hpp>
#include <ErrorHandling.hpp>
#include <PlanRewriteUtils.hpp>

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
/// Concrete operators can overwrite this behavior by overwriting Reorderer
namespace
{
Schema<Field, Ordered> heuristicOutputOrder(const LogicalOperator& visiting, const std::vector<LogicalOperator>& newChildren)
{
    /// For now we reuse the semantic field order of the operators as a heuristic for deciding the FieldOrdering trait
    /// Other strategies may be explored for better physical optimization.
    /// A child's ordering trait is read below. In a DAG a shared child may carry a target order imposed by another sink
    /// (see apply): a shared operator has a single physical output order that all of its parents -- across sink branches --
    /// must consume, so deriving this operator's order from that child is intentional, not an accidental cross-sink leak.
    const std::unordered_map<TypedLogicalOperator<>, Schema<Field, Ordered>> childrenWithOutputOrder
        = newChildren
        | std::views::transform(
              [](const auto& child) -> std::pair<TypedLogicalOperator<>, Schema<Field, Ordered>>
              {
                  return std::pair{
                      child,
                      child->getTraitSet().template get<FieldOrderingTrait>()->getOrderedFields() | RangeBinder{child}
                          | std::ranges::to<Schema<Field, Ordered>>()};
              })
        | std::ranges::to<std::unordered_map>();

    if (const auto reorderer = visiting.tryGetAs<Reorderer>())
    {
        /// The Reorderer callback is keyed by the original child, so map each original child to its rewritten counterpart.
        const auto oldChildren = visiting.getChildren();
        std::vector<std::pair<LogicalOperator, LogicalOperator>> oldWithNewChildren;
        oldWithNewChildren.reserve(oldChildren.size());
        for (std::size_t i = 0; i < oldChildren.size(); ++i)
        {
            oldWithNewChildren.emplace_back(oldChildren[i], newChildren[i]);
        }
        const auto childrenMap = oldWithNewChildren | std::ranges::to<std::unordered_map>();
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
}

}

const std::type_info& DecideFieldOrder::getType()
{
    return typeid(DecideFieldOrder);
}

std::string_view DecideFieldOrder::getName()
{
    return NAME;
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> DecideFieldOrder::dependsOn() const
{
    return {typeid(FixedPlanStructureBarrier)};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> DecideFieldOrder::requiredBy() const
{
    return {};
}

bool DecideFieldOrder::operator==(const DecideFieldOrder&) const
{
    return true;
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
LogicalPlan DecideFieldOrder::apply(const LogicalPlan& queryPlan) const
{
    PRECONDITION(not queryPlan.getRootOperators().empty(), "Query must have a sink root operator");

    /// Each sink imposes its target schema order on its child. A child shared between multiple sinks can only carry one
    /// field order, so all sinks sharing it must agree on the order.
    std::unordered_map<OperatorId, Schema<UnqualifiedUnboundField, Ordered>> targetOrders;
    for (const auto& rootOperator : queryPlan.getRootOperators())
    {
        auto rootSink = rootOperator.getAs<SinkLogicalOperator>();
        auto sinkDescriptorOpt = rootSink->getSinkDescriptor();
        PRECONDITION(sinkDescriptorOpt.has_value(), "Root sink must have a sink descriptor");
        auto targetSchema = *NES::get<std::shared_ptr<const Schema<UnqualifiedUnboundField, Ordered>>>(sinkDescriptorOpt->getSchema());
        const auto child = rootSink->getChild();
        for (const auto& targetField : targetSchema)
        {
            PRECONDITION(
                child.getOutputSchema().contains(targetField.getFullyQualifiedName()),
                "Field {} not present in root child output schema",
                targetField.getFullyQualifiedName());
        }
        if (const auto [existing, inserted] = targetOrders.try_emplace(child.getId(), targetSchema); !inserted)
        {
            if (existing->second != targetSchema)
            {
                throw UnsupportedQuery(
                    "sinks sharing the operator {} require conflicting field orders", child.explain(ExplainVerbosity::Short));
            }
        }
    }

    return rewritePlanBottomUp(
        queryPlan,
        [&targetOrders](const LogicalOperator& visiting, std::vector<LogicalOperator> newChildren) -> LogicalOperator
        {
            auto traitSet = visiting.getTraitSet();
            if (visiting.tryGetAs<SinkLogicalOperator>().has_value())
            {
                traitSet.insert(FieldOrderingTrait{Schema<UnqualifiedUnboundField, Ordered>{}});
            }
            else if (const auto targetOrder = targetOrders.find(visiting.getId()); targetOrder != targetOrders.end())
            {
                traitSet.insert(FieldOrderingTrait{targetOrder->second});
            }
            else
            {
                traitSet.insert(FieldOrderingTrait{unbind(heuristicOutputOrder(visiting, newChildren))});
            }
            return visiting.withTraitSet(std::move(traitSet)).withChildren(std::move(newChildren));
        });
}
}

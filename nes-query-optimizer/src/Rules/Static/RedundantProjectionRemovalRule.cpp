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

#include <Rules/Static/RedundantProjectionRemovalRule.hpp>

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
#include <DataTypes/UnboundField.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/Reorderer.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Barriers/FixedPlanStructureBarrier.hpp>
#include <Rules/Static/DecideFieldMappings.hpp>
#include <Rules/Static/DecideFieldOrder.hpp>
#include <Schema/Binder.hpp>
#include <Schema/Field.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

const std::type_info& RedundantProjectionRemovalRule::getType()
{
    return typeid(RedundantProjectionRemovalRule);
}

std::string_view RedundantProjectionRemovalRule::getName()
{
    return NAME;
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> RedundantProjectionRemovalRule::dependsOn() const
{
    return {};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> RedundantProjectionRemovalRule::requiredBy() const
{
    return {typeid(FixedPlanStructureBarrier)};
}

bool RedundantProjectionRemovalRule::operator==(const RedundantProjectionRemovalRule&) const
{
    return true;
}

namespace
{
Schema<Field, Ordered> calculateOutputOrder(const LogicalOperator& visiting)
{
    const auto childrenWithOutputOrder = visiting.getChildren()
        | std::views::transform([](const auto& child) { return std::pair{child, calculateOutputOrder(child)}; })
        | std::ranges::to<std::unordered_map>();

    if (const auto reorderer = visiting.tryGetAs<Reorderer>())
    {
        return reorderer.value()->get().getOrderedOutputSchema([&childrenWithOutputOrder](const LogicalOperator& child)
                                                               { return childrenWithOutputOrder.at(child); });
    }

    const auto orderedBoundInputSchema = visiting.getChildren()
        | std::views::transform([&](const auto& child) { return childrenWithOutputOrder.at(child); }) | std::views::join
        | std::ranges::to<Schema<Field, Ordered>>();

    std::vector<Field> outputOrder;
    std::vector<Field> rest;
    const auto outputSchema = visiting.getOutputSchema();
    for (const auto& inputField : orderedBoundInputSchema)
    {
        if (const auto& outputField = outputSchema[inputField.getFullyQualifiedName()])
        {
            outputOrder.push_back(outputField.value());
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
    outputOrder.insert(outputOrder.end(), rest.begin(), rest.end());
    return outputOrder | std::ranges::to<Schema<Field, Ordered>>();
}

LogicalOperator recur(const LogicalOperator& op)
{
    auto newChildren = op.getChildren() | std::views::transform(recur) | std::ranges::to<std::vector>();

    if (const auto projection = op.tryGetAs<ProjectionLogicalOperator>())
    {
        const auto trivial = [&]
        {
            INVARIANT(op.getChildren().size() == 1, "Projection operator must have exactly one child");
            for (const auto& [as, function] : projection.value()->getProjections())
            {
                const auto fieldAccessFunction = function.tryGetAs<FieldAccessLogicalFunction>();
                if (!fieldAccessFunction.has_value())
                {
                    return false;
                }
                if (fieldAccessFunction.value()->getField().getLastName() != as.getLastName())
                {
                    return false;
                }
            }
            const auto childOrder = unbind(calculateOutputOrder(op.getChildren().front()));
            const auto projectionOrder = unbind(
                projection.value()->getOrderedOutputSchema([](const LogicalOperator& child) { return calculateOutputOrder(child); }));
            return childOrder == projectionOrder;
        }();
        if (trivial)
        {
            return newChildren.front();
        }
    }
    return op.withChildren(std::move(newChildren));
}
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
LogicalPlan RedundantProjectionRemovalRule::apply(LogicalPlan queryPlan) const
{
    PRECONDITION(queryPlan.getRootOperators().size() == 1, "Query plan must have exactly one root operator");
    queryPlan = queryPlan.withRootOperators({recur(queryPlan.getRootOperators().front().withInferredSchema())});
    return queryPlan;
}

}

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
#include <Phases/PredicatePushdown.hpp>

#include <ranges>
#include <unordered_set>
#include <vector>

#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

std::unordered_set<std::string> PredicatePushdown::collectReferencedFields(const LogicalFunction& function)
{
    std::unordered_set<std::string> fields;
    for (const auto& node : BFSRange<LogicalFunction>(function))
    {
        if (const auto fieldAccess = node.tryGetAs<FieldAccessLogicalFunction>())
        {
            fields.insert(fieldAccess.value()->getFieldName());
        }
    }
    return fields;
}

bool PredicatePushdown::allFieldsBelongToSchema(const std::unordered_set<std::string>& fields, const Schema& schema)
{
    return std::ranges::all_of(fields, [&schema](const std::string& fieldName) { return schema.getFieldByName(fieldName).has_value(); });
}

LogicalPlan PredicatePushdown::apply(const LogicalPlan& queryPlan)
{
    PRECONDITION(queryPlan.getRootOperators().size() == 1, "Only single root operators are supported for now");
    PRECONDITION(not queryPlan.getRootOperators().empty(), "Query must have a sink root operator");
    return LogicalPlan{queryPlan.getQueryId(), {apply(queryPlan.getRootOperators()[0])}};
}

LogicalOperator PredicatePushdown::apply(const LogicalOperator& logicalOperator)
{
    /// First, recursively apply to all children
    const auto children = logicalOperator.getChildren()
        | std::views::transform([this](const LogicalOperator& child) { return apply(child); }) | std::ranges::to<std::vector>();
    auto current = logicalOperator.withChildren(children);

    /// Check if the current operator is a Selection
    const auto selectionOp = current.tryGetAs<SelectionLogicalOperator>();
    if (not selectionOp.has_value())
    {
        return current;
    }

    const auto& predicate = selectionOp.value()->getPredicate();
    const auto referencedFields = collectReferencedFields(predicate);

    /// The selection should have exactly one child
    const auto currentChildren = current.getChildren();
    if (currentChildren.size() != 1)
    {
        return current;
    }
    const auto& child = currentChildren[0];

    /// Rule 1: Push Selection past Projection
    /// A selection can be pushed below a projection if all fields referenced by the predicate
    /// are available in the projection's input (i.e., they exist in the schema before the projection).
    if (const auto projectionOp = child.tryGetAs<ProjectionLogicalOperator>())
    {
        /// Check that the predicate's fields exist in the projection's input schema
        const auto projInputSchemas = projectionOp.value()->getInputSchemas();
        if (not projInputSchemas.empty() and allFieldsBelongToSchema(referencedFields, projInputSchemas[0]))
        {
            NES_DEBUG("PredicatePushdown: pushing selection below projection (operator {})", child.getId());
            /// Swap: projection becomes the parent, selection moves below it
            /// New tree: Projection -> Selection -> Projection's children
            const auto projChildren = child.getChildren();
            auto newSelection = current.withChildren(projChildren);
            /// Re-apply pushdown on the newly positioned selection in case it can be pushed further
            newSelection = apply(newSelection);
            return child.withChildren({newSelection});
        }
        return current;
    }

    /// Rule 2: Push Selection past Join
    /// If the selection's predicate only references fields from one side of the join,
    /// push the selection to that side.
    if (const auto joinOp = child.tryGetAs<JoinLogicalOperator>())
    {
        const auto leftSchema = joinOp.value()->getLeftSchema();
        const auto rightSchema = joinOp.value()->getRightSchema();
        const auto joinChildren = child.getChildren();

        if (joinChildren.size() != 2)
        {
            return current;
        }

        const auto allLeft = allFieldsBelongToSchema(referencedFields, leftSchema);
        const auto allRight = allFieldsBelongToSchema(referencedFields, rightSchema);

        if (allLeft and not allRight)
        {
            NES_DEBUG("PredicatePushdown: pushing selection to left side of join (operator {})", child.getId());
            /// Push selection to the left child of the join
            auto newLeftChild = current.withChildren({joinChildren[0]});
            /// Re-apply pushdown on the newly positioned selection
            newLeftChild = apply(newLeftChild);
            return child.withChildren({newLeftChild, joinChildren[1]});
        }
        if (allRight and not allLeft)
        {
            NES_DEBUG("PredicatePushdown: pushing selection to right side of join (operator {})", child.getId());
            /// Push selection to the right child of the join
            auto newRightChild = current.withChildren({joinChildren[1]});
            /// Re-apply pushdown on the newly positioned selection
            newRightChild = apply(newRightChild);
            return child.withChildren({joinChildren[0], newRightChild});
        }
        /// If the predicate references fields from both sides, we cannot push it down
        return current;
    }

    return current;
}

}

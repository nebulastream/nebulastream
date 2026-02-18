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

#include <Functions/BooleanFunctions/AndLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/Sources/InlineSourceLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/Sources/SourceNameLogicalOperator.hpp>
#include <Operators/UnionLogicalOperator.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Operators/Windows/WindowedAggregationLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{
/// Extracts all field names referenced by a logical function via BFS over the function tree.
std::unordered_set<std::string> extractFieldNames(const LogicalFunction& function)
{
    std::unordered_set<std::string> fields;
    for (const auto& fn : BFSRange<LogicalFunction>(function))
    {
        if (const auto fieldAccess = fn.tryGetAs<FieldAccessLogicalFunction>())
        {
            fields.insert(fieldAccess.value()->getFieldName());
        }
    }
    return fields;
}

/// Recursively decomposes AND nodes into a flat list of conjuncts.
std::vector<LogicalFunction> splitAndPredicates(const LogicalFunction& function)
{
    if (function.tryGetAs<AndLogicalFunction>().has_value())
    {
        const auto children = function.getChildren();
        auto left = splitAndPredicates(children[0]);
        auto right = splitAndPredicates(children[1]);
        left.insert(left.end(), right.begin(), right.end());
        return left;
    }
    return {function};
}

/// Rebuilds a single predicate from a list of conjuncts using AND.
LogicalFunction combineWithAnd(const std::vector<LogicalFunction>& conjuncts)
{
    PRECONDITION(!conjuncts.empty(), "Cannot combine empty conjuncts");
    auto result = conjuncts[0];
    for (size_t i = 1; i < conjuncts.size(); ++i)
    {
        result = LogicalFunction{AndLogicalFunction(result, conjuncts[i])};
    }
    return result;
}

/// Checks whether all field names in the set exist in the given schema.
bool allFieldsInSchema(const std::unordered_set<std::string>& fields, const Schema& schema)
{
    return std::ranges::all_of(fields, [&schema](const std::string& field) { return schema.contains(field); });
}

/// Returns true if the operator is a source (leaf) operator that selections should not be pushed below.
bool isSourceOperator(const LogicalOperator& op)
{
    return op.tryGetAs<InlineSourceLogicalOperator>().has_value() || op.tryGetAs<SourceDescriptorLogicalOperator>().has_value()
        || op.tryGetAs<SourceNameLogicalOperator>().has_value();
}

/// Resolves the output schema of an operator by walking down to the nearest source if the operator's own schema is empty.
Schema resolveOutputSchema(const LogicalOperator& op)
{
    auto schema = op.getOutputSchema();
    if (schema.hasFields())
    {
        return schema;
    }
    /// Walk to a leaf to find a source with a real schema.
    if (!op.getChildren().empty())
    {
        return resolveOutputSchema(op.getChildren()[0]);
    }
    return schema;
}
}

LogicalPlan PredicatePushdown::apply(const LogicalPlan& queryPlan)
{
    PRECONDITION(queryPlan.getRootOperators().size() == 1, "Only single root operators are supported for now");
    PRECONDITION(not queryPlan.getRootOperators().empty(), "Query must have a sink root operator");
    return LogicalPlan{queryPlan.getQueryId(), {apply(queryPlan.getRootOperators()[0])}};
}

LogicalOperator PredicatePushdown::apply(const LogicalOperator& logicalOperator)
{
    /// First, recursively transform all children.
    auto children = logicalOperator.getChildren()
        | std::views::transform([this](const LogicalOperator& child) { return apply(child); }) | std::ranges::to<std::vector>();

    /// If the current operator is not a Selection, just rebuild with transformed children.
    const auto selectionOp = logicalOperator.tryGetAs<SelectionLogicalOperator>();
    if (!selectionOp.has_value())
    {
        return logicalOperator.withChildren(children);
    }

    /// Current operator is a Selection. Attempt to push it below its child.
    PRECONDITION(children.size() == 1, "Selection must have exactly one child");
    const auto& child = children[0];
    const auto predicateFields = extractFieldNames(selectionOp.value()->getPredicate());

    /// Do not push below sources or windowed aggregations.
    if (isSourceOperator(child) || child.tryGetAs<WindowedAggregationLogicalOperator>().has_value())
    {
        return logicalOperator.withChildren(children);
    }

    /// Push below Projection if all predicate fields exist in the Projection's input (child's child schema).
    if (child.tryGetAs<ProjectionLogicalOperator>().has_value())
    {
        PRECONDITION(!child.getChildren().empty(), "Projection must have at least one child");
        const auto grandchild = child.getChildren()[0];
        const auto grandchildSchema = resolveOutputSchema(grandchild);
        if (allFieldsInSchema(predicateFields, grandchildSchema))
        {
            /// Move selection below projection: Projection(Selection(grandchild))
            auto movedSelection = logicalOperator.withChildren({grandchild});
            return child.withChildren({movedSelection});
        }
        return logicalOperator.withChildren(children);
    }

    /// Push into Join branches by splitting AND conjuncts.
    if (child.tryGetAs<JoinLogicalOperator>().has_value())
    {
        auto joinChildren = child.getChildren();
        PRECONDITION(joinChildren.size() == 2, "Join must have exactly two children");

        /// Use the children's resolved output schemas to determine which side fields belong to.
        const auto leftSchema = resolveOutputSchema(joinChildren[0]);
        const auto rightSchema = resolveOutputSchema(joinChildren[1]);
        const auto conjuncts = splitAndPredicates(selectionOp.value()->getPredicate());

        std::vector<LogicalFunction> leftConjuncts, rightConjuncts, remainingConjuncts;
        for (const auto& conjunct : conjuncts)
        {
            const auto fields = extractFieldNames(conjunct);
            const bool allInLeft = allFieldsInSchema(fields, leftSchema);
            const bool allInRight = allFieldsInSchema(fields, rightSchema);
            if (allInLeft && !allInRight)
            {
                leftConjuncts.push_back(conjunct);
            }
            else if (allInRight && !allInLeft)
            {
                rightConjuncts.push_back(conjunct);
            }
            else
            {
                remainingConjuncts.push_back(conjunct);
            }
        }

        /// Push left-only predicates to the left child.
        if (!leftConjuncts.empty())
        {
            auto leftPredicate = combineWithAnd(leftConjuncts);
            auto leftSelection = LogicalOperator{SelectionLogicalOperator(leftPredicate)};
            joinChildren[0] = leftSelection.withChildren({joinChildren[0]});
        }

        /// Push right-only predicates to the right child.
        if (!rightConjuncts.empty())
        {
            auto rightPredicate = combineWithAnd(rightConjuncts);
            auto rightSelection = LogicalOperator{SelectionLogicalOperator(rightPredicate)};
            joinChildren[1] = rightSelection.withChildren({joinChildren[1]});
        }

        auto newJoin = child.withChildren(joinChildren);

        /// If there are remaining cross-side predicates, keep the selection above the join.
        if (!remainingConjuncts.empty())
        {
            auto remainingPredicate = combineWithAnd(remainingConjuncts);
            auto remainingSelection = LogicalOperator{SelectionLogicalOperator(remainingPredicate)};
            return remainingSelection.withChildren({newJoin});
        }
        return newJoin;
    }

    /// Duplicate selection into both Union branches.
    if (child.tryGetAs<UnionLogicalOperator>().has_value())
    {
        auto unionChildren = child.getChildren();
        PRECONDITION(unionChildren.size() == 2, "Union must have exactly two children");

        for (auto& unionChild : unionChildren)
        {
            auto duplicatedSelection = LogicalOperator{SelectionLogicalOperator(selectionOp.value()->getPredicate())};
            unionChild = duplicatedSelection.withChildren({unionChild});
        }

        return child.withChildren(unionChildren);
    }

    /// For other unary operators, push if predicate fields are available in grandchild output schema.
    if (child.getChildren().size() == 1)
    {
        const auto grandchild = child.getChildren()[0];
        const auto grandchildSchema = resolveOutputSchema(grandchild);
        if (allFieldsInSchema(predicateFields, grandchildSchema))
        {
            auto movedSelection = logicalOperator.withChildren({grandchild});
            return child.withChildren({movedSelection});
        }
    }

    return logicalOperator.withChildren(children);
}

}

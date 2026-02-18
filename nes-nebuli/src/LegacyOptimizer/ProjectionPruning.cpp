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
#include <LegacyOptimizer/ProjectionPruning.hpp>

#include <ranges>
#include <string>
#include <unordered_set>
#include <vector>

#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/Sinks/InlineSinkLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
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

/// Resolves the output schema of an operator by walking down to the nearest source if the operator's own schema is empty.
Schema resolveOutputSchema(const LogicalOperator& op)
{
    auto schema = op.getOutputSchema();
    if (schema.hasFields())
    {
        return schema;
    }
    if (!op.getChildren().empty())
    {
        return resolveOutputSchema(op.getChildren()[0]);
    }
    return schema;
}

/// Collects all field names from a schema into an unordered set.
std::unordered_set<std::string> schemaFieldNames(const Schema& schema)
{
    std::unordered_set<std::string> fields;
    for (const auto& name : schema.getFieldNames())
    {
        fields.insert(name);
    }
    return fields;
}

/// Returns true if the operator is a source (leaf) operator.
bool isSourceOperator(const LogicalOperator& op)
{
    return op.tryGetAs<InlineSourceLogicalOperator>().has_value() || op.tryGetAs<SourceDescriptorLogicalOperator>().has_value()
        || op.tryGetAs<SourceNameLogicalOperator>().has_value();
}

/// Creates a pruning projection that keeps only the specified fields from the child's output schema.
LogicalOperator createPruningProjection(const LogicalOperator& child, const std::unordered_set<std::string>& neededFields)
{
    const auto childSchema = resolveOutputSchema(child);
    std::vector<ProjectionLogicalOperator::Projection> projections;
    for (const auto& field : childSchema.getFields())
    {
        if (neededFields.contains(field.name))
        {
            projections.emplace_back(std::nullopt, LogicalFunction{FieldAccessLogicalFunction(field.name)});
        }
    }
    auto projection = LogicalOperator{ProjectionLogicalOperator(std::move(projections), ProjectionLogicalOperator::Asterisk(false))};
    return projection.withChildren({child});
}

/// Returns true if the child has fields not in the needed set (i.e., pruning would be beneficial).
bool hasSurplusFields(const Schema& childSchema, const std::unordered_set<std::string>& neededFields)
{
    return std::ranges::any_of(
        childSchema.getFieldNames(), [&neededFields](const std::string& name) { return !neededFields.contains(name); });
}
}

void ProjectionPruning::apply(LogicalPlan& queryPlan) const
{
    PRECONDITION(queryPlan.getRootOperators().size() == 1, "Only single root operators are supported for now");
    PRECONDITION(not queryPlan.getRootOperators().empty(), "Query must have a sink root operator");
    const auto root = queryPlan.getRootOperators()[0];
    /// Start with all fields needed. The root is typically a sink which has no output schema,
    /// so we resolve the schema from its first child (the actual data-producing operator).
    PRECONDITION(!root.getChildren().empty(), "Root operator must have at least one child");
    const auto allFields = schemaFieldNames(resolveOutputSchema(root.getChildren()[0]));
    queryPlan = LogicalPlan{queryPlan.getQueryId(), {apply(root, allFields)}};
}

LogicalOperator ProjectionPruning::apply(const LogicalOperator& logicalOperator, const std::unordered_set<std::string>& neededFields) const
{
    /// For source (leaf) operators, insert a pruning projection if there are surplus fields.
    if (isSourceOperator(logicalOperator))
    {
        const auto sourceSchema = resolveOutputSchema(logicalOperator);
        if (sourceSchema.hasFields() && hasSurplusFields(sourceSchema, neededFields))
        {
            return createPruningProjection(logicalOperator, neededFields);
        }
        return logicalOperator;
    }

    /// For sink operators, propagate all needed fields to children.
    if (logicalOperator.tryGetAs<InlineSinkLogicalOperator>().has_value() || logicalOperator.tryGetAs<SinkLogicalOperator>().has_value())
    {
        auto children = logicalOperator.getChildren()
            | std::views::transform([this, &neededFields](const LogicalOperator& child) { return apply(child, neededFields); })
            | std::ranges::to<std::vector>();
        return logicalOperator.withChildren(children);
    }

    /// For Selection, add predicate fields to the needed set and propagate.
    if (const auto selectionOp = logicalOperator.tryGetAs<SelectionLogicalOperator>())
    {
        auto expandedNeeded = neededFields;
        for (const auto& field : extractFieldNames(selectionOp.value()->getPredicate()))
        {
            expandedNeeded.insert(field);
        }
        auto children = logicalOperator.getChildren()
            | std::views::transform([this, &expandedNeeded](const LogicalOperator& child) { return apply(child, expandedNeeded); })
            | std::ranges::to<std::vector>();
        return logicalOperator.withChildren(children);
    }

    /// For Projection, compute needed fields from the projection expressions.
    if (const auto projectionOp = logicalOperator.tryGetAs<ProjectionLogicalOperator>())
    {
        std::unordered_set<std::string> accessedFields;
        for (const auto& fieldName : projectionOp.value()->getAccessedFields())
        {
            accessedFields.insert(fieldName);
        }
        auto children = logicalOperator.getChildren()
            | std::views::transform([this, &accessedFields](const LogicalOperator& child) { return apply(child, accessedFields); })
            | std::ranges::to<std::vector>();
        return logicalOperator.withChildren(children);
    }

    /// For Join, split needed fields by left/right schema + join condition fields.
    if (const auto joinOp = logicalOperator.tryGetAs<JoinLogicalOperator>())
    {
        auto joinChildren = logicalOperator.getChildren();
        PRECONDITION(joinChildren.size() == 2, "Join must have exactly two children");
        const auto leftSchema = resolveOutputSchema(joinChildren[0]);
        const auto rightSchema = resolveOutputSchema(joinChildren[1]);
        auto joinFields = extractFieldNames(joinOp.value()->getJoinFunction());

        std::unordered_set<std::string> leftNeeded, rightNeeded;
        for (const auto& field : neededFields)
        {
            if (leftSchema.contains(field))
            {
                leftNeeded.insert(field);
            }
            if (rightSchema.contains(field))
            {
                rightNeeded.insert(field);
            }
        }
        for (const auto& field : joinFields)
        {
            if (leftSchema.contains(field))
            {
                leftNeeded.insert(field);
            }
            if (rightSchema.contains(field))
            {
                rightNeeded.insert(field);
            }
        }

        auto leftChild = apply(joinChildren[0], leftNeeded);
        auto rightChild = apply(joinChildren[1], rightNeeded);

        return logicalOperator.withChildren({leftChild, rightChild});
    }

    /// For WindowedAggregation, do not prune (it defines its own output schema).
    if (logicalOperator.tryGetAs<WindowedAggregationLogicalOperator>().has_value())
    {
        auto children = logicalOperator.getChildren()
            | std::views::transform([this, &neededFields](const LogicalOperator& child) { return apply(child, neededFields); })
            | std::ranges::to<std::vector>();
        return logicalOperator.withChildren(children);
    }

    /// For Union, propagate same needed fields to both branches.
    if (logicalOperator.tryGetAs<UnionLogicalOperator>().has_value())
    {
        auto children = logicalOperator.getChildren()
            | std::views::transform([this, &neededFields](const LogicalOperator& child) { return apply(child, neededFields); })
            | std::ranges::to<std::vector>();
        return logicalOperator.withChildren(children);
    }

    /// Default: propagate needed fields unchanged.
    auto children = logicalOperator.getChildren()
        | std::views::transform([this, &neededFields](const LogicalOperator& child) { return apply(child, neededFields); })
        | std::ranges::to<std::vector>();
    return logicalOperator.withChildren(children);
}

}

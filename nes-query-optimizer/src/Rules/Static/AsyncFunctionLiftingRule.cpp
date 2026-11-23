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

#include <Rules/Static/AsyncFunctionLiftingRule.hpp>

#include <algorithm>
#include <cstddef>
#include <ranges>
#include <set>
#include <string>
#include <string_view>
#include <typeindex>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <Functions/AsyncCallLogicalFunction.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Operators/AsyncLogicalOperator.hpp>
#include <Operators/EventTimeWatermarkAssignerLogicalOperator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/Reorderer.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Barriers/SemanticAnalysisBarrier.hpp>
#include <Rules/PlanVisitor.hpp>
#include <Rules/Semantic/TypeInferenceRule.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <PlanRuleRegistry.hpp>

namespace NES
{

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> AsyncFunctionLiftingRule::needs() const
{
    return {typeid(TypeInferenceRule)};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> AsyncFunctionLiftingRule::neededBy() const
{
    return {typeid(SemanticAnalysisBarrier)};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> AsyncFunctionLiftingRule::wants() const
{
    /// In particular, never depend on a pushdown: those require SemanticAnalysisBarrier.
    return {};
}

namespace
{
using UnboundProjection = ProjectionLogicalOperator::UnboundProjection;
/// Parser/inference desugaring (IN/BETWEEN/IS NAN) shares one AsyncCallLogicalFunction DAG node
/// across several expression edges. Resolve shared pointer references to one produced field only
/// within their evaluation region, never by expression structure, arguments, types or text, and
/// never across clauses/operators.
using RegionOutputs = std::unordered_map<TypedLogicalFunction<AsyncCallLogicalFunction>, Identifier>;

bool containsAsync(const LogicalFunction& function)
{
    return std::ranges::any_of(
        BFSRange(function), [](const LogicalFunction& node) { return node.tryGetAs<AsyncCallLogicalFunction>().has_value(); });
}

bool subtreeHasAsyncWork(const LogicalOperator& root)
{
    std::vector<LogicalOperator> pending{root};
    std::unordered_set<LogicalOperator> visited;
    while (!pending.empty())
    {
        auto op = std::move(pending.back());
        pending.pop_back();
        if (!visited.insert(op).second)
        {
            continue;
        }
        if (op.tryGetAs<AsyncLogicalOperator>())
        {
            return true;
        }
        if (const auto projection = op.tryGetAs<ProjectionLogicalOperator>())
        {
            if (std::ranges::any_of(projection.value()->getProjections(), [](const auto& entry) { return containsAsync(entry.second); }))
            {
                return true;
            }
        }
        if (const auto selection = op.tryGetAs<SelectionLogicalOperator>(); selection && containsAsync(selection.value()->getPredicate()))
        {
            return true;
        }
        const auto children = op.getChildren();
        pending.insert(pending.end(), children.begin(), children.end());
    }
    return false;
}

void rejectSharedAsyncSubplans(const LogicalPlan& plan)
{
    auto pending = plan.getRootOperators();
    std::unordered_set<LogicalOperator> visited;
    std::unordered_map<LogicalOperator, size_t> consumers;
    while (!pending.empty())
    {
        auto op = std::move(pending.back());
        pending.pop_back();
        if (!visited.insert(op).second)
        {
            continue;
        }
        for (const auto& child : op.getChildren())
        {
            ++consumers[child];
            pending.push_back(child);
        }
    }
    for (const auto& [op, count] : consumers)
    {
        if (count > 1 && subtreeHasAsyncWork(op))
        {
            throw UnsupportedQuery(
                "Shared async subplans with multiple consumers are not supported; reuse output fields within one branch");
        }
    }
}

bool isPlainArgument(const LogicalFunction& function)
{
    return function.tryGetAs<FieldAccessLogicalFunction>().has_value() || function.tryGetAs<ConstantValueLogicalFunction>().has_value();
}

/// Resolve logical column order before traits exist, using CalcTargetOrderRule's exact
/// Reorderer/default mapping. Cache by operator identity, including shared DAG children.
class OrderedSchemas
{
public:
    Schema<Field, Ordered> get(const LogicalOperator& op)
    {
        if (const auto cached = schemas.find(op); cached != schemas.end())
        {
            return cached->second;
        }
        if (const auto reorderer = op.tryGetAs<Reorderer>())
        {
            auto schema = reorderer.value()->get().getOrderedOutputSchema([this](const LogicalOperator& child) { return get(child); });
            schemas.emplace(op, schema);
            return schema;
        }

        /// Preserve child order, not unordered-map iteration order.
        const auto children = op.getChildren();
        const auto inputSchema = children | std::views::transform([this](const LogicalOperator& child) { return get(child); })
            | std::views::join | std::ranges::to<Schema<Field, Ordered>>();
        const auto outputSchema = op.getOutputSchema();
        std::vector<Field> outputOrder;
        std::vector<Field> rest;
        for (const auto& inputField : inputSchema)
        {
            if (const auto outputField = outputSchema[inputField.getFullyQualifiedName()])
            {
                outputOrder.push_back(outputField.value());
            }
        }
        for (const auto& outputField : outputSchema)
        {
            if (!inputSchema.contains(outputField.getFullyQualifiedName()))
            {
                rest.push_back(outputField);
            }
        }
        std::ranges::sort(
            rest,
            [](const Field& lhs, const Field& rhs)
            { return fmt::format("{}", lhs.getFullyQualifiedName()) < fmt::format("{}", rhs.getFullyQualifiedName()); });
        outputOrder.insert(outputOrder.end(), rest.begin(), rest.end());
        auto schema = std::move(outputOrder) | std::ranges::to<Schema<Field, Ordered>>();
        schemas.emplace(op, schema);
        return schema;
    }

private:
    std::unordered_map<LogicalOperator, Schema<Field, Ordered>> schemas;
};

FieldAccessLogicalFunction accessOutput(const LogicalOperator& op, const Identifier& name)
{
    const auto field = op.getOutputSchema()[name];
    INVARIANT(field.has_value(), "normalized async input must produce field {}", name);
    return FieldAccessLogicalFunction{field.value()};
}

std::vector<UnboundProjection> forwardAll(const LogicalOperator& op, OrderedSchemas& orderedSchemas)
{
    std::vector<UnboundProjection> result;
    for (const auto& field : orderedSchemas.get(op))
    {
        result.emplace_back(field.getLastName(), FieldAccessLogicalFunction{field});
    }
    return result;
}

class AsyncNormalizer
{
public:
    void reserve(const LogicalOperator& op)
    {
        if (op.tryGetAs<SinkLogicalOperator>())
        {
            return;
        }
        /// Scan the entire original plan before allocating anything, including aliases above calls.
        for (const auto& field : op.getOutputSchema())
        {
            names.insert(field.getLastName().asCanonicalString());
        }
    }

    LogicalOperator normalize(const LogicalOperator& original, const std::vector<LogicalOperator>& children)
    {
        auto current = original.withChildren(children);
        if (const auto projection = current.tryGetAs<ProjectionLogicalOperator>())
        {
            const auto entries = projection.value()->getProjections();
            if (std::ranges::none_of(entries, [](const auto& entry) { return containsAsync(entry.second); }))
            {
                return current;
            }
            /// Snapshot before appending helpers, not after rewriting any expression.
            const auto visible = orderedSchemas.get(original.getAs<ProjectionLogicalOperator>()->getChild());
            auto input = projection.value()->getChild();
            RegionOutputs regionOutputs;
            std::vector<UnboundProjection> rewritten;
            if (projection.value()->hasAsterisk())
            {
                for (const auto& field : visible)
                {
                    rewritten.emplace_back(field.getLastName(), accessOutput(input, field.getLastName()));
                }
            }
            for (const auto& [field, expression] : entries)
            {
                rewritten.emplace_back(field.getLastName(), rewrite(expression, input, regionOutputs));
            }
            return ProjectionLogicalOperator::create(input, rewritten, ProjectionLogicalOperator::Asterisk{false})
                .withTraitSet(current.getTraitSet());
        }
        if (const auto selection = current.tryGetAs<SelectionLogicalOperator>();
            selection && containsAsync(selection.value()->getPredicate()))
        {
            const auto visible = orderedSchemas.get(original.getAs<SelectionLogicalOperator>()->getChild());
            auto input = selection.value()->getChild();
            RegionOutputs regionOutputs;
            const auto predicate = rewrite(selection.value()->getPredicate(), input, regionOutputs);
            const LogicalOperator filtered = SelectionLogicalOperator::create(input, predicate).withTraitSet(current.getTraitSet());
            std::vector<UnboundProjection> restored;
            for (const auto& field : visible)
            {
                restored.emplace_back(field.getLastName(), accessOutput(filtered, field.getLastName()));
            }
            return ProjectionLogicalOperator::create(filtered, restored, ProjectionLogicalOperator::Asterisk{false});
        }
        if (const auto async = current.tryGetAs<AsyncLogicalOperator>())
        {
            /// PlanVisitor visits each original DAG node once, even with multiple parents.
            /// Newly emitted operators are not revisited during this pass.
            auto input = async.value()->getChild();
            const auto visible = orderedSchemas.get(original);
            RegionOutputs regionOutputs;
            std::vector<LogicalFunction> arguments;
            bool changed = false;
            for (const auto& argument : async.value()->getArguments())
            {
                changed |= containsAsync(argument) || !isPlainArgument(argument);
                arguments.push_back(materialize(rewrite(argument, input, regionOutputs), input));
            }
            if (!changed)
            {
                /// Already normalized: do not add another restoring projection on later passes.
                return current;
            }
            /// withArguments/withChildren preserve all multioutput bindings.
            const LogicalOperator normalized = async.value()->withChildrenUnsafe({input}).withArguments(arguments);
            std::vector<UnboundProjection> restored;
            for (const auto& field : visible)
            {
                restored.emplace_back(field.getLastName(), accessOutput(normalized, field.getLastName()));
            }
            return ProjectionLogicalOperator::create(normalized, restored, ProjectionLogicalOperator::Asterisk{false});
        }
        /// These are the other operator APIs exposing arbitrary scalar expressions. Window
        /// grouping, aggregation inputs and time characteristics are field-only APIs.
        if (const auto join = current.tryGetAs<JoinLogicalOperator>())
        {
            rejectUnsupported(join.value()->getJoinFunction(), "join predicate");
        }
        if (const auto watermark = current.tryGetAs<EventTimeWatermarkAssignerLogicalOperator>())
        {
            rejectUnsupported(watermark.value()->getOnField(), "watermark expression");
        }
        return current;
    }

private:
    OrderedSchemas orderedSchemas;
    size_t counter = 0;
    std::set<std::string> names;
    /// Compare only instance identity (pointer equality), never expression structure or arguments.
    std::unordered_set<TypedLogicalFunction<AsyncCallLogicalFunction>> seenCalls;

    void claim(const TypedLogicalFunction<AsyncCallLogicalFunction>& call)
    {
        if (!seenCalls.insert(call).second)
        {
            throw InvalidQuerySyntax(
                "Async normalization: reused async call node across independent evaluation regions; share a bound output field instead");
        }
    }

    static void rejectUnsupported(const LogicalFunction& expression, std::string_view context)
    {
        if (containsAsync(expression))
        {
            throw InvalidQuerySyntax("Async normalization: async calls are unsupported in {}", context);
        }
    }

    Identifier freshName(std::string_view kind)
    {
        while (true)
        {
            auto name = Identifier::parse(fmt::format("__async_{}_{}", kind, counter++));
            if (names.insert(name.asCanonicalString()).second)
            {
                return name;
            }
        }
    }

    LogicalFunction materialize(const LogicalFunction& argument, LogicalOperator& input)
    {
        if (isPlainArgument(argument))
        {
            return argument;
        }
        const auto name = freshName("arg");
        auto projections = forwardAll(input, orderedSchemas);
        projections.emplace_back(name, argument);
        input = ProjectionLogicalOperator::create(input, projections, ProjectionLogicalOperator::Asterisk{false});
        return accessOutput(input, name);
    }

    LogicalFunction rewrite(const LogicalFunction& expression, LogicalOperator& input, RegionOutputs& regionOutputs)
    {
        /// CASE is lazy: hoisting any of its descendants would introduce eager effects.
        if (expression.getType() == "Conditional")
        {
            rejectUnsupported(expression, "conditional/lazy expressions");
            return expression;
        }
        const auto call = expression.tryGetAs<AsyncCallLogicalFunction>();
        if (call)
        {
            if (const auto produced = regionOutputs.find(*call); produced != regionOutputs.end())
            {
                /// Resolve shared DAG references before visiting their children: a shared
                /// outer call must not execute or claim any nested calls again.
                return accessOutput(input, produced->second);
            }
            /// First encounter in this region must still be unique across the whole plan.
            claim(*call);
        }
        std::vector<LogicalFunction> arguments;
        for (const auto& child : expression.getChildren())
        {
            auto rewritten = rewrite(child, input, regionOutputs);
            /// NOLINTNEXTLINE(clang-analyzer-cplusplus.Move) each iteration constructs a fresh wrapper; only the non-call arm moves it
            arguments.push_back(call ? materialize(rewritten, input) : std::move(rewritten));
        }
        if (!call)
        {
            return expression.withChildren(arguments);
        }
        const auto binding = freshName("call");
        input = AsyncLogicalOperator::create(input, call.value()->getAsyncOperator(), std::move(arguments), {binding});
        regionOutputs.emplace(*call, binding);
        return accessOutput(input, binding);
    }
};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
LogicalPlan AsyncFunctionLiftingRule::apply(LogicalPlan queryPlan) const
{
    /// Occurrence tracking is pass-local: repeated normalization preserves existing invocations.
    rejectSharedAsyncSubplans(queryPlan);
    AsyncNormalizer normalizer;
    PlanVisitor<> reserveNames{
        [&normalizer](const LogicalOperator& op, const std::vector<LogicalOperator>&) -> PlanVisitor<>::UpResult
        {
            normalizer.reserve(op);
            return op;
        }};
    queryPlan = reserveNames.apply(std::move(queryPlan));
    PlanVisitor<> visitor{
        [&normalizer](const LogicalOperator& op, const std::vector<LogicalOperator>& children) -> PlanVisitor<>::UpResult
        { return normalizer.normalize(op, children); }};
    return visitor.apply(std::move(queryPlan));
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param)
PlanRuleRegistryReturnType AsyncFunctionLiftingRule::create(PlanRuleRegistryArguments)
{
    return AsyncFunctionLiftingRule{};
}

}

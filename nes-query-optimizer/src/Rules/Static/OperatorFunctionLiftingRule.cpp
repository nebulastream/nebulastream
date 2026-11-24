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

#include <Rules/Static/OperatorFunctionLiftingRule.hpp>

#include <algorithm>
#include <cstddef>
#include <optional>
#include <ranges>
#include <set>
#include <string>
#include <string_view>
#include <typeindex>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Functions/OperatorCallLogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Operators/EventTimeWatermarkAssignerLogicalOperator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Barriers/SemanticAnalysisBarrier.hpp>
#include <Rules/PlanVisitor.hpp>
#include <Rules/Semantic/CalcTargetOrderRule.hpp>
#include <Rules/Semantic/TypeInferenceRule.hpp>
#include <Rules/Static/DecideFieldOrder.hpp>
#include <Schema/Binder.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Traits/FieldOrderingTrait.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <OperatorFunctionRegistry.hpp>
#include <PlanRuleRegistry.hpp>

namespace NES
{

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> OperatorFunctionLiftingRule::needs() const
{
    return {typeid(TypeInferenceRule), typeid(CalcTargetOrderRule)};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> OperatorFunctionLiftingRule::neededBy() const
{
    return {typeid(SemanticAnalysisBarrier)};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> OperatorFunctionLiftingRule::wants() const
{
    /// In particular, never depend on a pushdown: those require SemanticAnalysisBarrier.
    return {};
}

namespace
{
using UnboundProjection = ProjectionLogicalOperator::UnboundProjection;
/// Parser/inference desugaring (IN/BETWEEN/IS NAN) shares one operator-call DAG node
/// across several expression edges. Resolve shared pointer references to one produced field only
/// within their evaluation region, never by expression structure, arguments, types or text, and
/// never across clauses/operators.
using RegionOutputs = std::unordered_map<TypedLogicalFunction<OperatorCallLogicalFunction>, Identifier>;

bool containsOperatorCall(const LogicalFunction& function)
{
    return std::ranges::any_of(
        BFSRange(function), [](const LogicalFunction& node) { return node.tryGetAs<OperatorCallLogicalFunction>().has_value(); });
}

bool isPlainArgument(const LogicalFunction& function)
{
    return function.tryGetAs<FieldAccessLogicalFunction>().has_value() || function.tryGetAs<ConstantValueLogicalFunction>().has_value();
}

Schema<Field, Ordered> orderedFields(const LogicalOperator& op)
{
    return op.getTraitSet().get<FieldOrderingTrait>()->getOrderedFields() | RangeBinder{op} | std::ranges::to<Schema<Field, Ordered>>();
}

FieldAccessLogicalFunction accessOutput(const LogicalOperator& op, const Identifier& name)
{
    const auto field = op.getOutputSchema()[name];
    INVARIANT(field.has_value(), "normalized operator input must produce field {}", name);
    return FieldAccessLogicalFunction{field.value()};
}

std::vector<UnboundProjection> forwardAll(const LogicalOperator& op)
{
    std::vector<UnboundProjection> result;
    for (const auto& field : orderedFields(op))
    {
        result.emplace_back(field.getLastName(), FieldAccessLogicalFunction{field});
    }
    return result;
}

class OperatorFunctionNormalizer
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
            if (std::ranges::none_of(entries, [](const auto& entry) { return containsOperatorCall(entry.second); }))
            {
                return current;
            }
            /// Snapshot before appending helpers, not after rewriting any expression.
            const auto visible = orderedFields(original.getAs<ProjectionLogicalOperator>()->getChild());
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
                rewritten.emplace_back(field.getLastName(), rewrite(expression, input, regionOutputs, field.getLastName()));
            }
            return ProjectionLogicalOperator::create(input, rewritten, ProjectionLogicalOperator::Asterisk{false})
                .withTraitSet(current.getTraitSet());
        }
        if (const auto selection = current.tryGetAs<SelectionLogicalOperator>();
            selection && containsOperatorCall(selection.value()->getPredicate()))
        {
            const auto visible = orderedFields(original.getAs<SelectionLogicalOperator>()->getChild());
            auto input = selection.value()->getChild();
            RegionOutputs regionOutputs;
            const auto predicate = rewrite(selection.value()->getPredicate(), input, regionOutputs);
            const auto filtered = DecideFieldOrder::withInferredFieldOrder(
                SelectionLogicalOperator::create(input, predicate).withTraitSet(current.getTraitSet()));
            std::vector<UnboundProjection> restored;
            for (const auto& field : visible)
            {
                restored.emplace_back(field.getLastName(), accessOutput(filtered, field.getLastName()));
            }
            return DecideFieldOrder::withInferredFieldOrder(
                ProjectionLogicalOperator::create(filtered, restored, ProjectionLogicalOperator::Asterisk{false}));
        }
        if (const auto configured = findOperatorFunction(current))
        {
            /// PlanVisitor visits each original DAG node once, even with multiple parents.
            /// Newly emitted operators are not revisited during this pass.
            auto input = current.getChildren().at(0);
            const auto visible = orderedFields(original);
            RegionOutputs regionOutputs;
            std::vector<LogicalFunction> arguments;
            bool changed = false;
            for (const auto& argument : configured->second)
            {
                changed |= containsOperatorCall(argument) || !isPlainArgument(argument);
                arguments.push_back(materialize(rewrite(argument, input, regionOutputs), input));
            }
            if (!changed)
            {
                /// Already normalized: do not add another restoring projection on later passes.
                return current;
            }
            const auto definition = OperatorFunctionRegistry::instance().find(configured->first);
            INVARIANT(definition.has_value(), "Operator function definition disappeared during normalization");
            const auto normalized = DecideFieldOrder::withInferredFieldOrder(
                definition->createOperator(input, arguments).withTraitSet(current.getTraitSet()));
            std::vector<UnboundProjection> restored;
            for (const auto& field : visible)
            {
                restored.emplace_back(field.getLastName(), accessOutput(normalized, field.getLastName()));
            }
            return DecideFieldOrder::withInferredFieldOrder(
                ProjectionLogicalOperator::create(normalized, restored, ProjectionLogicalOperator::Asterisk{false}));
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
    size_t counter = 0;
    std::set<std::string> names;
    /// Compare only instance identity (pointer equality), never expression structure or arguments.
    std::unordered_set<TypedLogicalFunction<OperatorCallLogicalFunction>> seenCalls;

    void claim(const TypedLogicalFunction<OperatorCallLogicalFunction>& call)
    {
        if (!seenCalls.insert(call).second)
        {
            throw InvalidQuerySyntax(
                "Operator function normalization: reused call node across independent evaluation regions; share a named output field "
                "instead");
        }
    }

    static void rejectUnsupported(const LogicalFunction& expression, std::string_view context)
    {
        if (containsOperatorCall(expression))
        {
            throw InvalidQuerySyntax("Operator function calls are unsupported in {}", context);
        }
    }

    Identifier freshName(std::string_view kind)
    {
        while (true)
        {
            auto name = Identifier::parse(fmt::format("__operator_{}_{}", kind, counter++));
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
        auto projections = forwardAll(input);
        projections.emplace_back(name, argument);
        input = DecideFieldOrder::withInferredFieldOrder(
            ProjectionLogicalOperator::create(input, projections, ProjectionLogicalOperator::Asterisk{false}));
        return accessOutput(input, name);
    }

    LogicalOperator appendCall(
        const OperatorCallLogicalFunction& call,
        std::vector<LogicalFunction> arguments,
        LogicalOperator input,
        const Identifier& resultName)
    {
        const auto originalFields = orderedFields(input);
        const auto declaredResultName = call.getOutputName();
        std::optional<Identifier> renamedInput;
        if (input.getOutputSchema().contains(declaredResultName))
        {
            /// Temporarily rename a colliding input with an ordinary projection. The
            /// operator still appends its declared result, and the projection below restores the input.
            renamedInput = freshName("input");
            std::vector<UnboundProjection> projections;
            for (const auto& field : originalFields)
            {
                const auto name = field.getLastName();
                projections.emplace_back(name == declaredResultName ? *renamedInput : name, FieldAccessLogicalFunction{field});
            }
            input = DecideFieldOrder::withInferredFieldOrder(
                ProjectionLogicalOperator::create(input, std::move(projections), ProjectionLogicalOperator::Asterisk{false}));
            for (auto& argument : arguments)
            {
                if (const auto field = argument.tryGetAs<FieldAccessLogicalFunction>();
                    field && field.value()->getField().getLastName() == declaredResultName)
                {
                    argument = accessOutput(input, *renamedInput);
                }
            }
        }
        const auto invocation = DecideFieldOrder::withInferredFieldOrder(call.withChildren(arguments).toLogicalOperator(input));
        if (!renamedInput && resultName == declaredResultName)
        {
            return invocation;
        }
        std::vector<UnboundProjection> projections;
        for (const auto& field : originalFields)
        {
            const auto name = field.getLastName();
            const auto inputName = renamedInput && name == declaredResultName ? *renamedInput : name;
            projections.emplace_back(name, accessOutput(invocation, inputName));
        }
        projections.emplace_back(resultName, accessOutput(invocation, declaredResultName));
        return DecideFieldOrder::withInferredFieldOrder(
            ProjectionLogicalOperator::create(invocation, std::move(projections), ProjectionLogicalOperator::Asterisk{false}));
    }

    LogicalFunction rewrite(const LogicalFunction& expression, LogicalOperator& input, RegionOutputs& regionOutputs)
    {
        return rewrite(expression, input, regionOutputs, std::nullopt);
    }

    LogicalFunction rewrite(
        const LogicalFunction& expression,
        LogicalOperator& input,
        RegionOutputs& regionOutputs,
        const std::optional<Identifier>& preferredResultName)
    {
        /// CASE is lazy: hoisting any of its descendants would introduce eager effects.
        if (expression.getType() == "Conditional")
        {
            rejectUnsupported(expression, "conditional/lazy expressions");
            return expression;
        }
        const auto call = expression.tryGetAs<OperatorCallLogicalFunction>();
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
        names.insert(call.value()->getOutputName().asCanonicalString());
        const auto outputName
            = preferredResultName && !input.getOutputSchema().contains(*preferredResultName) ? *preferredResultName : freshName("call");
        input = appendCall(call.value().get(), std::move(arguments), input, outputName);
        regionOutputs.emplace(*call, outputName);
        return accessOutput(input, outputName);
    }
};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
LogicalPlan OperatorFunctionLiftingRule::apply(LogicalPlan queryPlan) const
{
    /// Occurrence tracking is pass-local: repeated normalization preserves existing invocations.
    queryPlan = DecideFieldOrder{}.apply(queryPlan);
    OperatorFunctionNormalizer normalizer;
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
PlanRuleRegistryReturnType OperatorFunctionLiftingRule::create(PlanRuleRegistryArguments)
{
    return OperatorFunctionLiftingRule{};
}

}

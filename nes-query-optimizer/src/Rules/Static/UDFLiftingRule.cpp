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

#include <Rules/Static/UDFLiftingRule.hpp>

#include <algorithm>
#include <cstddef>
#include <filesystem>
#include <optional>
#include <ranges>
#include <set>
#include <string>
#include <string_view>
#include <tuple>
#include <typeindex>
#include <typeinfo>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Functions/UDFCallLogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/UDFLogicalOperator.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Barriers/FixedPlanStructureBarrier.hpp>
#include <Rules/Barriers/SemanticAnalysisBarrier.hpp>
#include <Rules/PlanVisitor.hpp>
#include <Rules/Static/PredicatePushdownRule.hpp>
#include <Rules/Static/ProjectionPushdownRule.hpp>
#include <Rules/Static/RedundantProjectionRemovalRule.hpp>
#include <Schema/Field.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <PlanRuleRegistry.hpp>
#include <UdfDescriptor.hpp>

namespace NES
{

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> UDFLiftingRule::needs() const
{
    return {typeid(SemanticAnalysisBarrier)};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> UDFLiftingRule::neededBy() const
{
    return {typeid(FixedPlanStructureBarrier)};
}

/// The pushdown rules do not know the UDF operator.
/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> UDFLiftingRule::wants() const
{
    return {typeid(PredicatePushdownRule), typeid(ProjectionPushdownRule), typeid(RedundantProjectionRemovalRule)};
}

namespace
{
using UnboundProjection = ProjectionLogicalOperator::UnboundProjection;
using Substitution = std::vector<std::pair<LogicalFunction, LogicalFunction>>;

/// Calls can only be fused if they run the same way and through the same library.
using FusionKey = std::tuple<UdfExecution, UdfLanguage, std::filesystem::path>;

bool isLifted(const LogicalFunction& function)
{
    const auto call = function.tryGetAs<UDFCallLogicalFunction>();
    return call.has_value() && call.value()->getDescriptor().has_value()
        && call.value()->getDescriptor()->getExecution() != UdfExecution::Inline;
}

bool containsLifted(const LogicalFunction& function)
{
    return std::ranges::any_of(BFSRange(function), isLifted);
}

FusionKey fusionKey(const LogicalFunction& call)
{
    const auto& descriptor = call.tryGetAs<UDFCallLogicalFunction>().value()->getDescriptor();
    return {descriptor->getExecution(), descriptor->getLanguage(), descriptor->getPath()};
}

/// Collects the calls that read no other lifted call, once each: the same call on the same fields computes the same value.
/// If `key` is unset, the first such call sets it, and only calls that can be fused with it are collected.
void collectReadyCalls(const LogicalFunction& function, std::optional<FusionKey>& key, std::vector<LogicalFunction>& out)
{
    if (isLifted(function) && std::ranges::none_of(function.getChildren(), containsLifted))
    {
        if (!key.has_value())
        {
            key = fusionKey(function);
        }
        if (fusionKey(function) == *key && std::ranges::find(out, function) == out.end())
        {
            out.push_back(function);
        }
        return;
    }
    for (const auto& child : function.getChildren())
    {
        collectReadyCalls(child, key, out);
    }
}

bool isPlainArgument(const LogicalFunction& function)
{
    return function.tryGetAs<FieldAccessLogicalFunction>().has_value() || function.tryGetAs<ConstantValueLogicalFunction>().has_value();
}

/// Field accesses that survive are rebound by name when the projection above infers its schema.
LogicalFunction substitute(const LogicalFunction& function, const Substitution& substitution)
{
    if (const auto it = std::ranges::find(substitution, function, &Substitution::value_type::first); it != substitution.end())
    {
        return it->second;
    }
    return function.withChildren(
        function.getChildren() | std::views::transform([&](const LogicalFunction& child) { return substitute(child, substitution); })
        | std::ranges::to<std::vector>());
}

FieldAccessLogicalFunction accessOutput(const LogicalOperator& op, const Identifier& name)
{
    const auto field = op.getOutputSchema()[name];
    INVARIANT(field.has_value(), "lifted operator must produce field {}", name);
    return FieldAccessLogicalFunction{field.value()};
}

/// Sorted by name: the schema is unordered, and the rewritten plan has to be deterministic.
std::vector<Field> sortedFields(const LogicalOperator& op)
{
    auto fields = op.getOutputSchema() | std::ranges::to<std::vector>();
    std::ranges::sort(fields, {}, [](const Field& field) { return field.getLastName().asCanonicalString(); });
    return fields;
}

std::vector<UnboundProjection> forwardAll(const LogicalOperator& op)
{
    return sortedFields(op)
        | std::views::transform([](const Field& field)
                                { return UnboundProjection{field.getLastName(), FieldAccessLogicalFunction{field}}; })
        | std::ranges::to<std::vector>();
}

class Lifter
{
public:
    LogicalOperator lift(const LogicalOperator& current)
    {
        if (const auto join = current.tryGetAs<JoinLogicalOperator>(); join.has_value() && containsLifted(join.value()->getJoinFunction()))
        {
            throw QueryInvalid(
                "A join predicate cannot call a UDF with EXECUTION IN_PROCESS or OUT_OF_PROCESS, as it needs both sides of the join. "
                "Use EXECUTION INLINE, or compute the UDF after the join: {}",
                join.value()->getJoinFunction().explain(ExplainVerbosity::Short));
        }
        if (const auto selection = current.tryGetAs<SelectionLogicalOperator>();
            selection.has_value() && containsLifted(selection.value()->getPredicate()))
        {
            return liftSelection(selection.value());
        }
        if (const auto projection = current.tryGetAs<ProjectionLogicalOperator>(); projection.has_value()
            && std::ranges::any_of(projection.value()->getUnboundProjections(),
                                   [](const auto& entry) { return containsLifted(entry.second); }))
        {
            return liftProjection(projection.value());
        }
        return current;
    }

private:
    size_t counter = 0;

    /// Unique per plan, and never one of the names the stage forwards.
    Identifier freshName(const std::string_view kind, const LogicalOperator& input)
    {
        while (true)
        {
            auto name = Identifier::parse(fmt::format("__udf_{}_{}", kind, counter++));
            if (!input.getOutputSchema()[name].has_value())
            {
                return name;
            }
        }
    }

    /// The predicate becomes a field of a projection, which is lifted like any other:
    ///   child -> projection [child fields, predicate] (lifted) -> selection on the field -> projection [child fields]
    LogicalOperator liftSelection(const TypedLogicalOperator<SelectionLogicalOperator>& selection)
    {
        const auto child = selection->getChild();
        const auto predicateName = freshName("pred", child);
        auto evaluation = forwardAll(child);
        evaluation.emplace_back(predicateName, selection->getPredicate());
        const auto evaluated
            = liftProjection(ProjectionLogicalOperator::create(child, evaluation, ProjectionLogicalOperator::Asterisk{false}));

        const LogicalOperator filtered
            = SelectionLogicalOperator::create(evaluated, accessOutput(evaluated, predicateName)).withTraitSet(selection->getTraitSet());
        std::vector<UnboundProjection> restored;
        for (const auto& field : sortedFields(child))
        {
            restored.emplace_back(field.getLastName(), accessOutput(filtered, field.getLastName()));
        }
        return ProjectionLogicalOperator::create(filtered, restored, ProjectionLogicalOperator::Asterisk{false});
    }

    /// Adds UDF stages below the projection until no lifted call is left in its expressions. A call that reads another
    /// lifted call ends up one stage above it, as do calls that cannot be fused with the stage's first call.
    LogicalOperator liftProjection(const TypedLogicalOperator<ProjectionLogicalOperator>& projection)
    {
        /// Expanded against the child, so that the helper fields of the stages do not leak into the output.
        auto expressions = projection->hasAsterisk() ? forwardAll(projection->getChild()) : std::vector<UnboundProjection>{};
        std::ranges::copy(projection->getUnboundProjections(), std::back_inserter(expressions));

        auto input = projection->getChild();
        while (std::ranges::any_of(expressions, [](const auto& entry) { return containsLifted(entry.second); }))
        {
            input = addStage(input, expressions);
        }
        return ProjectionLogicalOperator::create(input, expressions, ProjectionLogicalOperator::Asterisk{false})
            .withTraitSet(projection->getTraitSet());
    }

    /// input -> pre-projection -> UDF operator. Rewrites `expressions` to read the results of the operator it returns.
    LogicalOperator addStage(const LogicalOperator& input, std::vector<UnboundProjection>& expressions)
    {
        std::vector<LogicalFunction> calls;
        std::optional<FusionKey> key;
        for (const auto& [name, function] : expressions)
        {
            collectReadyCalls(function, key, calls);
        }

        /// pre: only what the stage or the expressions above it read, so that a scan does not parse the other fields of a
        /// source. A record needs at least one field, even if the calls only read constants.
        std::unordered_set<Identifier> readFields;
        for (const auto& [name, function] : expressions)
        {
            for (const auto& node : BFSRange(function))
            {
                if (const auto access = node.template tryGetAs<FieldAccessLogicalFunction>())
                {
                    readFields.insert(access.value()->getField().getLastName());
                }
            }
        }
        auto preProjections = forwardAll(input)
            | std::views::filter([&](const UnboundProjection& forwarded) { return readFields.contains(forwarded.first); })
            | std::ranges::to<std::vector>();
        if (preProjections.empty())
        {
            preProjections.push_back(forwardAll(input).front());
        }
        std::vector<std::pair<LogicalFunction, Identifier>> computedArguments;
        for (const auto& call : calls)
        {
            for (const auto& argument : call.getChildren())
            {
                if (!isPlainArgument(argument)
                    && std::ranges::find(computedArguments, argument, &decltype(computedArguments)::value_type::first)
                        == computedArguments.end())
                {
                    auto name = freshName("arg", input);
                    preProjections.emplace_back(name, argument);
                    computedArguments.emplace_back(argument, std::move(name));
                }
            }
        }
        /// Only a projection creates the scan of a source, so a source always gets one.
        const bool repeatsInput = computedArguments.empty() && preProjections.size() == input.getOutputSchema().size()
            && !input.tryGetAs<SourceDescriptorLogicalOperator>().has_value();
        const LogicalOperator pre
            = repeatsInput ? input : ProjectionLogicalOperator::create(input, preProjections, ProjectionLogicalOperator::Asterisk{false});

        Substitution argumentSubstitution;
        for (const auto& [argument, name] : computedArguments)
        {
            argumentSubstitution.emplace_back(argument, accessOutput(pre, name));
        }
        std::vector<UDFLogicalOperator::NamedCall> namedCalls;
        for (const auto& call : calls)
        {
            namedCalls.emplace_back(
                freshName("call", pre),
                call.withChildren(
                    call.getChildren()
                    | std::views::transform([&](const LogicalFunction& argument) { return substitute(argument, argumentSubstitution); })
                    | std::ranges::to<std::vector>()));
        }
        const LogicalOperator stage = UDFLogicalOperator::create(pre, namedCalls);

        Substitution callSubstitution;
        for (const auto& [call, namedCall] : std::views::zip(calls, namedCalls))
        {
            callSubstitution.emplace_back(call, accessOutput(stage, namedCall.first));
        }
        for (auto& [name, function] : expressions)
        {
            function = substitute(function, callSubstitution);
        }
        return stage;
    }
};
}

LogicalPlan UDFLiftingRule::apply(LogicalPlan queryPlan) const /// NOLINT(readability-convert-member-functions-to-static)
{
    Lifter lifter;
    PlanVisitor<> visitor{PlanVisitor<>::FunctionUp{
        [&lifter](
            const LogicalOperator& op,
            std::vector<LogicalOperator> children,
            std::monostate,
            const std::unordered_map<LogicalOperator, std::monostate>&) -> PlanVisitor<>::UpResult
        { return lifter.lift(op.withChildren(std::move(children))); }}};
    return visitor.apply(std::move(queryPlan));
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param)
PlanRuleRegistryReturnType UDFLiftingRule::create(PlanRuleRegistryArguments)
{
    return UDFLiftingRule{};
}

}

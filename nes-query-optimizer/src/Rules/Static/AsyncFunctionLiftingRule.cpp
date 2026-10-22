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
#include <typeinfo>
#include <unordered_map>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <Functions/AsyncLogicalFunction.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Barriers/FixedPlanStructureBarrier.hpp>
#include <Rules/Barriers/SemanticAnalysisBarrier.hpp>
#include <Rules/PlanVisitor.hpp>
#include <Rules/Static/PredicatePushdownRule.hpp>
#include <Rules/Static/ProjectionPushdownRule.hpp>
#include <Rules/Static/RedundantProjectionRemovalRule.hpp>
#include <Schema/Field.hpp>
#include <Traits/AsyncExecutionTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <PlanRuleRegistry.hpp>

namespace NES
{

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> AsyncFunctionLiftingRule::needs() const
{
    return {typeid(SemanticAnalysisBarrier)};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> AsyncFunctionLiftingRule::neededBy() const
{
    return {typeid(FixedPlanStructureBarrier)};
}

/// The pushdown rules rebuild projections from scratch and would drop the AsyncExecutionTrait.
/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> AsyncFunctionLiftingRule::wants() const
{
    return {typeid(PredicatePushdownRule), typeid(ProjectionPushdownRule), typeid(RedundantProjectionRemovalRule)};
}

namespace
{
using UnboundProjection = ProjectionLogicalOperator::UnboundProjection;
using Substitution = std::vector<std::pair<LogicalFunction, LogicalFunction>>;
using Named = std::vector<std::pair<LogicalFunction, Identifier>>;

bool isAsync(const LogicalFunction& function)
{
    return function.tryGetAs<AsyncLogicalFunction>().has_value();
}

bool containsAsync(const LogicalFunction& function)
{
    return std::ranges::any_of(BFSRange(function), isAsync);
}

/// What the async stage can read without evaluating anything.
bool isPlainArgument(const LogicalFunction& function)
{
    return function.tryGetAs<FieldAccessLogicalFunction>().has_value() || function.tryGetAs<ConstantValueLogicalFunction>().has_value();
}

/// A call whose arguments are each either free of async calls, or a ready call themselves. A ready call
/// and the calls it reads run in one async stage; sync work between two calls ends the chain, because the
/// async stage cannot evaluate it.
bool isReady(const LogicalFunction& function)
{
    return isAsync(function)
        && std::ranges::all_of(
               function.getChildren(), [](const LogicalFunction& argument) { return !containsAsync(argument) || isReady(argument); });
}

/// The outermost ready calls. Structurally equal calls are collected once and therefore evaluated once.
void collectReadyCalls(const LogicalFunction& function, std::vector<LogicalFunction>& out)
{
    if (isReady(function))
    {
        if (std::ranges::find(out, function) == out.end())
        {
            out.push_back(function);
        }
        return;
    }
    for (const auto& child : function.getChildren())
    {
        collectReadyCalls(child, out);
    }
}

/// The arguments of a call chain that the pre-projection has to compute: sync work that is neither a field nor a constant.
void collectComputedArguments(const LogicalFunction& call, std::vector<LogicalFunction>& out)
{
    for (const auto& argument : call.getChildren())
    {
        if (isAsync(argument))
        {
            collectComputedArguments(argument, out);
        }
        else if (!isPlainArgument(argument) && std::ranges::find(out, argument) == out.end())
        {
            out.push_back(argument);
        }
    }
}

/// Identifies a call by its expression over field names. Fields that are forwarded by name hold the same
/// data, so two calls with the same key compute the same values.
std::string callKey(const LogicalFunction& call)
{
    return call.explain(ExplainVerbosity::Short);
}

/// Field accesses that survive are rebound by name when the projection infers its schema.
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
    /// One stage per chain of directly nested calls: each pass lifts the outermost ready calls.
    LogicalOperator liftAll(LogicalOperator current)
    {
        if (const auto selection = current.tryGetAs<SelectionLogicalOperator>();
            selection.has_value() && containsAsync(selection.value()->getPredicate()))
        {
            return liftSelection(selection.value());
        }
        if (const auto projection = current.tryGetAs<ProjectionLogicalOperator>())
        {
            if (const auto filter = filters.find(projection.value()->getChild()); filter != filters.end())
            {
                current = reuseFilterCalls(projection.value(), filter->second);
            }
        }
        while (const auto projection = current.tryGetAs<ProjectionLogicalOperator>())
        {
            if (hasTrait<AsyncExecutionTrait>(projection.value()->getTraitSet())
                || std::ranges::none_of(
                    projection.value()->getProjections(), [](const auto& entry) { return containsAsync(entry.second); }))
            {
                break;
            }
            current = liftReadyCalls(projection.value());
        }
        return current;
    }

private:
    /// A lifted selection whose call results a projection directly above it can read instead of calling again.
    struct LiftedFilter
    {
        LogicalOperator filtered;
        std::vector<std::pair<std::string, Identifier>> resultFields;
    };

    size_t counter = 0;
    /// Set while a predicate is lifted: the result fields of its calls are kept for the operators above.
    bool keepResultFields = false;
    std::vector<std::pair<std::string, Identifier>> liftedCalls;
    /// Keyed by the projection that restores a lifted selection's output.
    std::unordered_map<LogicalOperator, LiftedFilter> filters;

    /// Unique per plan, and never one of the names the stage forwards.
    Identifier freshName(std::string_view kind, const LogicalOperator& input)
    {
        while (true)
        {
            auto name = Identifier::parse(fmt::format("__async_{}_{}", kind, counter++));
            if (!input.getOutputSchema()[name].has_value())
            {
                return name;
            }
        }
    }

    /// The predicate becomes a field of a projection, which is lifted like any other:
    ///   child -> projection [child fields, predicate] (lifted) -> selection on the field -> projection [child fields]
    /// The selection still carries the call results; the last projection drops them so the output is the
    /// selection's, unless a projection above reads them instead (`reuseFilterCalls`).
    LogicalOperator liftSelection(const TypedLogicalOperator<SelectionLogicalOperator>& selection)
    {
        const auto child = selection->getChild();
        const auto predicateName = freshName("pred", child);
        auto evaluation = forwardAll(child);
        evaluation.emplace_back(predicateName, selection->getPredicate());

        liftedCalls.clear();
        keepResultFields = true;
        const auto evaluated = liftAll(ProjectionLogicalOperator::create(child, evaluation, ProjectionLogicalOperator::Asterisk{false}));
        keepResultFields = false;

        const LogicalOperator filtered
            = SelectionLogicalOperator::create(evaluated, accessOutput(evaluated, predicateName)).withTraitSet(selection->getTraitSet());
        std::vector<UnboundProjection> restored;
        for (const auto& field : sortedFields(child))
        {
            restored.emplace_back(field.getLastName(), accessOutput(filtered, field.getLastName()));
        }
        const LogicalOperator restoring = ProjectionLogicalOperator::create(filtered, restored, ProjectionLogicalOperator::Asterisk{false});
        filters.emplace(restoring, LiftedFilter{.filtered = filtered, .resultFields = std::exchange(liftedCalls, {})});
        return restoring;
    }

    /// Rebases a projection from the restoring projection of a lifted selection onto the selection itself,
    /// and reads the calls the predicate already made instead of making them again.
    LogicalOperator reuseFilterCalls(const TypedLogicalOperator<ProjectionLogicalOperator>& projection, const LiftedFilter& filter)
    {
        const auto reuse = [&](this const auto& self, const LogicalFunction& function) -> LogicalFunction
        {
            if (isAsync(function))
            {
                if (const auto result
                    = std::ranges::find(filter.resultFields, callKey(function), &std::pair<std::string, Identifier>::first);
                    result != filter.resultFields.end())
                {
                    return accessOutput(filter.filtered, result->second);
                }
            }
            return function.withChildren(function.getChildren() | std::views::transform(self) | std::ranges::to<std::vector>());
        };
        std::vector<UnboundProjection> rebased;
        if (projection->hasAsterisk())
        {
            for (const auto& field : sortedFields(projection->getChild()))
            {
                rebased.emplace_back(field.getLastName(), accessOutput(filter.filtered, field.getLastName()));
            }
        }
        for (const auto& [field, function] : projection->getProjections())
        {
            rebased.emplace_back(field.getLastName(), reuse(function));
        }
        return ProjectionLogicalOperator::create(filter.filtered, rebased, ProjectionLogicalOperator::Asterisk{false})
            .withTraitSet(projection->getTraitSet());
    }

    /// child -> pre (sync) -> async stage -> post (sync).
    LogicalOperator liftReadyCalls(const TypedLogicalOperator<ProjectionLogicalOperator>& projection)
    {
        const auto child = projection->getChild();
        std::vector<LogicalFunction> calls;
        for (const auto& [field, function] : projection->getProjections())
        {
            collectReadyCalls(function, calls);
        }

        /// pre: every argument the async stage cannot read directly becomes a field, and every call gets an
        /// empty slot of its result type. Handed-off rows then already have the async stage's output shape,
        /// so the stage only fills in the slots and one input buffer always fits one output buffer. Calls
        /// nested in a chain need no slot: their results only feed the call around them.
        auto preProjections = forwardAll(child);
        std::vector<LogicalFunction> computedArguments;
        for (const auto& call : calls)
        {
            collectComputedArguments(call, computedArguments);
        }
        Named liftedArguments;
        for (const auto& argument : computedArguments)
        {
            auto name = freshName("arg", child);
            preProjections.emplace_back(name, argument);
            liftedArguments.emplace_back(argument, std::move(name));
        }
        Named callNames;
        for (const auto& call : calls)
        {
            auto name = freshName("call", child);
            /// ponytail: "" or "0" covers VARSIZED and numeric results; BOOLEAN results need their own default
            const auto emptySlot = call.getDataType().isType(DataType::Type::VARSIZED) ? "" : "0";
            preProjections.emplace_back(name, ConstantValueLogicalFunction{call.getDataType(), emptySlot});
            liftedCalls.emplace_back(callKey(call), name);
            callNames.emplace_back(call, std::move(name));
        }
        const LogicalOperator pre = ProjectionLogicalOperator::create(child, preProjections, ProjectionLogicalOperator::Asterisk{false});

        /// async stage: forwards pre and fills each slot with its call.
        Substitution argumentSubstitution;
        for (const auto& [argument, name] : liftedArguments)
        {
            argumentSubstitution.emplace_back(argument, accessOutput(pre, name));
        }
        std::vector<UnboundProjection> asyncProjections;
        for (const auto& field : sortedFields(pre))
        {
            const auto call = std::ranges::find(callNames, field.getLastName(), &Named::value_type::second);
            asyncProjections.emplace_back(
                field.getLastName(),
                call == callNames.end() ? LogicalFunction{FieldAccessLogicalFunction{field}}
                                        : substitute(call->first, argumentSubstitution));
        }
        const auto asyncStage = ProjectionLogicalOperator::create(pre, asyncProjections, ProjectionLogicalOperator::Asterisk{false});
        auto traitSet = asyncStage->getTraitSet();
        tryInsert(traitSet, AsyncExecutionTrait{});
        const LogicalOperator async = asyncStage.withTraitSet(std::move(traitSet));

        /// post: the original projection on top of the async stage. An asterisk is expanded against
        /// the original child, so the helper fields do not leak into the output.
        Substitution callSubstitution;
        for (const auto& [call, name] : callNames)
        {
            callSubstitution.emplace_back(call, accessOutput(async, name));
        }
        std::vector<UnboundProjection> postProjections;
        if (projection->hasAsterisk())
        {
            for (const auto& field : sortedFields(child))
            {
                postProjections.emplace_back(field.getLastName(), accessOutput(async, field.getLastName()));
            }
        }
        for (const auto& [field, function] : projection->getProjections())
        {
            postProjections.emplace_back(field.getLastName(), substitute(function, callSubstitution));
        }
        if (keepResultFields)
        {
            for (const auto& [call, name] : callNames)
            {
                postProjections.emplace_back(name, accessOutput(async, name));
            }
        }
        auto post = ProjectionLogicalOperator::create(async, postProjections, ProjectionLogicalOperator::Asterisk{false});
        return post.withTraitSet(projection->getTraitSet());
    }
};
}

LogicalPlan AsyncFunctionLiftingRule::apply(LogicalPlan queryPlan) const /// NOLINT(readability-convert-member-functions-to-static)
{
    Lifter lifter;
    PlanVisitor<> visitor{
        [&lifter](const LogicalOperator& op, const std::vector<LogicalOperator>& children) -> PlanVisitor<>::UpResult
        { return lifter.liftAll(op.withChildren(children)); }};
    return visitor.apply(std::move(queryPlan));
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param)
PlanRuleRegistryReturnType AsyncFunctionLiftingRule::create(PlanRuleRegistryArguments)
{
    return AsyncFunctionLiftingRule{};
}

}

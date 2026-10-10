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

#include <HttpCallAsyncRule.hpp>

#include <algorithm>
#include <cstddef>
#include <optional>
#include <set>
#include <string>
#include <typeindex>
#include <typeinfo>
#include <unordered_map>
#include <utility>
#include <vector>

#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/HttpCallLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Barriers/FixedPlanStructureBarrier.hpp>
#include <Rules/PlanVisitor.hpp>
#include <Rules/Static/ProjectionPushdownRule.hpp>
#include <Rules/Static/RedundantProjectionRemovalRule.hpp>
#include <Schema/Field.hpp>
#include <Traits/AsyncExecutionTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/PlanRenderer.hpp>
#include <ErrorHandling.hpp>
#include <PlanRuleRegistry.hpp>

namespace NES
{

namespace
{

/// Enough room that the producer keeps the executor's threads busy without parking a large share of
/// the shared buffer pool: two buffers per concurrent call, never fewer than the framework's own
/// default. The same reasoning, and the same number, as the semantic resolution rule's.
constexpr size_t MinimumChannelCapacity = 64;

/// The name the asynchronous record layout stores. Unqualified, because the schema that travels
/// over the handoff channel is a `Schema<UnqualifiedUnboundField, Ordered>`.
std::string canonicalName(const Field& field)
{
    return field.getLastName().asCanonicalString();
}

bool containsHttpCall(const LogicalFunction& function)
{
    if (function.tryGetAs<HttpCallLogicalFunction>().has_value())
    {
        return true;
    }
    return std::ranges::any_of(function.getChildren(), [](const LogicalFunction& child) { return containsHttpCall(child); });
}

/// The marker, if this projection is one the framework can take over. Nullopt means "not ours";
/// anything that looks like an attempt but cannot be executed throws, because silently leaving it
/// to lowering would fail with a message about a missing physical function.
std::optional<AsyncExecutionTrait> asyncExecutionFor(const ProjectionLogicalOperator& projection, const LogicalOperator& op)
{
    const auto projections = projection.getProjections();

    std::optional<size_t> callIndex;
    for (size_t index = 0; index < projections.size(); ++index)
    {
        const auto& [target, expression] = projections[index];
        if (expression.tryGetAs<HttpCallLogicalFunction>().has_value())
        {
            if (callIndex.has_value())
            {
                throw UnsupportedQuery(
                    "A query may run one HTTP_CALL per projection, because each one becomes its own asynchronous operator: {}",
                    op.explain(ExplainVerbosity::Debug));
            }
            callIndex = index;
        }
        else if (containsHttpCall(expression))
        {
            throw UnsupportedQuery(
                "HTTP_CALL has to be the whole projected expression, because the operator that computes it is replaced by a source and "
                "nothing is left to apply '{}' to: {}",
                target.getLastName().asCanonicalString(),
                op.explain(ExplainVerbosity::Debug));
        }
    }
    if (!callIndex.has_value())
    {
        return std::nullopt;
    }

    /// Every other field has to come through untouched. The asynchronous source copies the input
    /// record into the output record **by field name** and the executor writes only the one field
    /// it computed, so a renamed or computed field would end up at its default value.
    for (size_t index = 0; index < projections.size(); ++index)
    {
        if (index == *callIndex)
        {
            continue;
        }
        const auto& [target, expression] = projections[index];
        const auto access = expression.tryGetAs<FieldAccessLogicalFunction>();
        if (!access.has_value())
        {
            throw UnsupportedQuery(
                "A projection that runs HTTP_CALL may only pass other fields through, but '{}' is computed: {}",
                target.getLastName().asCanonicalString(),
                op.explain(ExplainVerbosity::Debug));
        }
        if (canonicalName(access->get().getField()) != canonicalName(target))
        {
            throw UnsupportedQuery(
                "A projection that runs HTTP_CALL may not rename the fields it passes through, but '{}' is renamed to '{}': {}",
                canonicalName(access->get().getField()),
                canonicalName(target),
                op.explain(ExplainVerbosity::Debug));
        }
    }

    const auto& [outputField, expression] = projections[*callIndex];
    const auto& call = expression.tryGetAs<HttpCallLogicalFunction>()->get();
    const auto inputAccess = call.getInput().tryGetAs<FieldAccessLogicalFunction>();
    if (!inputAccess.has_value())
    {
        throw UnsupportedQuery(
            "HTTP_CALL sends the value of one field, so its first argument has to be a field rather than '{}': {}",
            call.getInput().explain(ExplainVerbosity::Short),
            op.explain(ExplainVerbosity::Debug));
    }

    std::unordered_map<std::string, std::string> config{
        {"url", call.getUrl()},
        {"input_field", canonicalName(inputAccess->get().getField())},
        {"output_field", canonicalName(outputField)},
        {"timeout_seconds", std::to_string(call.getTimeoutSeconds())}};

    NES_DEBUG(
        "HTTP_CALL on field '{}' into '{}' at {} runs asynchronously with {} calls in flight",
        config.at("input_field"),
        config.at("output_field"),
        config.at("url"),
        call.getMaxConcurrency());

    return AsyncExecutionTrait{
        "HttpCall",
        std::move(config),
        /// One record per request: this operator's protocol has no way to batch several.
        1,
        call.getMaxConcurrency(),
        std::max(MinimumChannelCapacity, 2 * call.getMaxConcurrency()),
        /// The demo asserts exact output, and input order is what makes that possible.
        true};
}

}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
LogicalPlan HttpCallAsyncRule::apply(LogicalPlan queryPlan) const
{
    PlanVisitor<> visitor{
        [](const LogicalOperator& op, std::vector<LogicalOperator> children) -> PlanVisitor<>::UpResult
        {
            /// The one misuse worth naming: there is no physical function, so lowering would
            /// otherwise complain about a missing registry entry instead of about the query.
            if (const auto selection = op.tryGetAs<SelectionLogicalOperator>())
            {
                if (containsHttpCall(selection->get().getPredicate()))
                {
                    throw UnsupportedQuery(
                        "HTTP_CALL can only be used in a SELECT list, not in a WHERE clause: its result is produced by a separate "
                        "asynchronous operator, which cannot sit inside a predicate: {}",
                        op.explain(ExplainVerbosity::Debug));
                }
            }

            if (const auto projection = op.tryGetAs<ProjectionLogicalOperator>())
            {
                if (auto trait = asyncExecutionFor(projection->get(), op))
                {
                    auto traits = op.getTraitSet();
                    traits.insert(std::move(trait).value());
                    return op.withTraitSet(std::move(traits)).withChildren(std::move(children));
                }
            }
            return op.withChildren(std::move(children));
        }};
    return visitor.apply(std::move(queryPlan));
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> HttpCallAsyncRule::needs() const
{
    /// After the rules that rewrite projections: this one marks a projection for replacement, and a
    /// projection that is still going to be pushed down or merged away is the wrong one to mark.
    /// Both of those run after the semantic analysis barrier, so field accesses are bound by then.
    return {typeid(ProjectionPushdownRule), typeid(RedundantProjectionRemovalRule)};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> HttpCallAsyncRule::neededBy() const
{
    return {typeid(FixedPlanStructureBarrier)};
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param)
PlanRuleRegistryReturnType HttpCallAsyncRule::create(PlanRuleRegistryArguments)
{
    return HttpCallAsyncRule{};
}

}

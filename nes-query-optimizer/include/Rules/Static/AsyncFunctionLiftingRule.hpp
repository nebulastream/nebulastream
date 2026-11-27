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

#pragma once

#include <set>
#include <string_view>
#include <typeindex>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Rule.hpp>
#include <PlanRuleRegistry.hpp>

namespace NES
{

/// Splits every projection that calls an `AsyncLogicalFunction` into
///   pre (sync) -> async stage -> post (sync)
/// The pre-projection materializes every async argument that is neither a field nor a constant,
/// so the async stage evaluates nothing but the async calls themselves, and reserves an empty slot
/// per call. The async stage is a projection carrying `AsyncExecutionTrait` that forwards its input
/// and fills each slot with its call, so its input and output have the same fields. The
/// post-projection is the original projection reading those fields.
///
/// A selection whose predicate calls an async function evaluates the predicate into a field of such a
/// projection first, filters on that field, and projects the helper fields away again. A projection directly
/// above reads the calls the predicate already made instead of making them again.
///
/// Directly nested calls (`f(g(x))`) run in one async stage. Sync work between two calls (`f(CONCAT(g(x), y))`)
/// needs a stage per side, with the sync work evaluated in the projection between them.
class AsyncFunctionLiftingRule
{
public:
    static PlanRuleRegistryReturnType create(PlanRuleRegistryArguments arguments);
    static constexpr std::string_view NAME = "AsyncFunctionLiftingRule";

    [[nodiscard]] LogicalPlan apply(LogicalPlan queryPlan) const;
    [[nodiscard]] std::set<std::type_index> needs() const;
    [[nodiscard]] std::set<std::type_index> neededBy() const;
    [[nodiscard]] std::set<std::type_index> wants() const;
};

static_assert(RuleConcept<AsyncFunctionLiftingRule, LogicalPlan>);
}

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
#include <typeinfo>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Rule.hpp>
#include <PlanRuleRegistry.hpp>

namespace NES
{

/// Heads every branch leaving a fan-out point with an OriginSplit operator, so that each branch carries origin ids of
/// its own. Which ids those are is decided by the OriginIdInferenceRule, which runs after this rule.
///
/// An operator read by more than one consumer hands the same records to each of them. Without a split, both branches
/// forward those records under the origin id they were read with, and anything that sees both of them again — an
/// operator where the branches merge, or a node receiving one network channel per branch — sees two streams under one
/// origin id. Sequence numbers are unique only within an origin, so those streams cannot be told apart: watermarks
/// and the per-origin sequence tracking of an emit both break on it.
///
/// Whether the branches meet again on one node or over the network is a placement decision, so the split is inserted
/// at every fan-out point rather than only where a branch crosses a node boundary: otherwise the same query would
/// work or fail depending on where the optimizer placed its operators.
class OriginSplitInsertionRule
{
public:
    static PlanRuleRegistryReturnType create(PlanRuleRegistryArguments arguments);
    static constexpr std::string_view NAME = "OriginSplitInsertionRule";

    [[nodiscard]] LogicalPlan apply(const LogicalPlan& queryPlan) const;

    /// A split infers its schema from the operator it heads, so it is inserted into a bound plan.
    [[nodiscard]] std::set<std::type_index> needs() const;

    /// Inserts operators, so it runs while the plan structure may still change.
    [[nodiscard]] std::set<std::type_index> neededBy() const;
};

static_assert(RuleConcept<OriginSplitInsertionRule, LogicalPlan>);
}

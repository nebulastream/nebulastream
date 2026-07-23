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

#include <functional>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Schema/Field.hpp>

namespace NES
{

/// recursively replaces field accesses of given LogicalFunction if they are a key in given field map with the field the key points to
LogicalFunction replaceFieldAccesses(const LogicalFunction& function, const std::unordered_map<Field, Field>& fields);

/// Returns the ids of all operators that are reachable through more than one parent edge in the (possibly DAG-shaped) plan.
/// Duplicate edges from the same parent (e.g. a union reading the same input twice) count as sharing as well.
[[nodiscard]] std::unordered_set<OperatorId> getSharedOperatorIds(const LogicalPlan& plan);

/// Shared state for one pushdown-rule invocation over a (possibly DAG-shaped) plan. A shared operator (reachable through
/// more than one parent, see getSharedOperatorIds) is a pushdown barrier: it is rewritten once with empty pending state
/// and reused by every parent, which re-introduces its own pending state above the barrier — so one parent's pushdown
/// cannot corrupt what a sibling parent reads through the same shared subtree.
struct PushdownBarrier
{
    std::unordered_set<OperatorId> sharedOperatorIds;
    std::unordered_map<OperatorId, LogicalOperator> rewrittenSharedOperators;

    explicit PushdownBarrier(const LogicalPlan& plan) : sharedOperatorIds(getSharedOperatorIds(plan)) { }

    [[nodiscard]] bool isShared(const LogicalOperator& op) const { return sharedOperatorIds.contains(op.getId()); }

    /// Rewrites a shared operator at most once (memoized) and reuses it for every parent. `rewriteWithoutPending()`
    /// produces the canonical rewrite; `materializeAbove(rewritten)` re-introduces the calling parent's pending state.
    template <typename RewriteWithoutPending, typename MaterializeAbove>
    LogicalOperator
    rewriteShared(const LogicalOperator& op, RewriteWithoutPending&& rewriteWithoutPending, MaterializeAbove&& materializeAbove)
    {
        auto rewritten = rewrittenSharedOperators.find(op.getId());
        if (rewritten == rewrittenSharedOperators.end())
        {
            rewritten = rewrittenSharedOperators.emplace(op.getId(), std::forward<RewriteWithoutPending>(rewriteWithoutPending)()).first;
        }
        return std::forward<MaterializeAbove>(materializeAbove)(rewritten->second);
    }
};

/// Rewrites all operators of a (possibly DAG-shaped) plan bottom-up. `rewrite` is invoked once per unique operator (keyed
/// on the input OperatorId) with the original operator and its already-rewritten children; the result is memoized and reused
/// for every parent, so shared operators stay shared. Deterministic order: children left-to-right, roots in order.
[[nodiscard]] LogicalPlan rewritePlanBottomUp(
    const LogicalPlan& plan,
    const std::function<LogicalOperator(const LogicalOperator& original, std::vector<LogicalOperator> rewrittenChildren)>& rewrite);

}

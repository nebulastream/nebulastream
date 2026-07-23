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

/// Shared state for a single pushdown rule invocation over a (possibly DAG-shaped) plan.
///
/// In a DAG, an operator reachable through more than one parent (a "shared" operator, see getSharedOperatorIds) acts as a
/// pushdown barrier: the state a pushdown rule threads downward (predicates, required fields, watermark assigners) must not
/// enter a subtree that another parent also reads, or one parent's rewrite would corrupt what the other observes. A shared
/// operator is therefore rewritten exactly once, with an empty pending state, and every parent reuses that single rewritten
/// instance -- preserving sharing -- while re-introducing its own pending state above the barrier.
struct PushdownBarrier
{
    std::unordered_set<OperatorId> sharedOperatorIds;
    std::unordered_map<OperatorId, LogicalOperator> rewrittenSharedOperators;

    explicit PushdownBarrier(const LogicalPlan& plan) : sharedOperatorIds(getSharedOperatorIds(plan)) { }

    [[nodiscard]] bool isShared(const LogicalOperator& op) const { return sharedOperatorIds.contains(op.getId()); }

    /// Rewrites a shared operator at most once and reuses the result for every parent.
    /// `rewriteWithoutPending()` produces the canonical rewrite of the shared subtree; it is invoked only when the first
    /// parent reaches the operator and the result is memoized. `materializeAbove(rewritten)` re-introduces the calling
    /// parent's pending state on top of that cached rewrite and is applied once per parent.
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

/// Rewrites all operators of a (possibly DAG-shaped) plan bottom-up.
/// The rewrite function is invoked exactly once per unique operator (identified by the input plan's OperatorId) with the original
/// operator and its already-rewritten children. The result is memoized and reused for every additional parent, so operators shared
/// between multiple parents stay shared in the rewritten plan. Children are rewritten left-to-right and roots in order, making the
/// invocation order deterministic for stateful rewrite functions.
[[nodiscard]] LogicalPlan rewritePlanBottomUp(
    const LogicalPlan& plan,
    const std::function<LogicalOperator(const LogicalOperator& original, std::vector<LogicalOperator> rewrittenChildren)>& rewrite);

}

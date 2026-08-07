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
#include <vector>

#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
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

/// Rewrites all operators of a (possibly DAG-shaped) plan bottom-up.
/// The rewrite function is invoked exactly once per unique operator (identified by the input plan's OperatorId) with the original
/// operator and its already-rewritten children. The result is memoized and reused for every additional parent, so operators shared
/// between multiple parents stay shared in the rewritten plan. Children are rewritten left-to-right and roots in order, making the
/// invocation order deterministic for stateful rewrite functions.
[[nodiscard]] LogicalPlan rewritePlanBottomUp(
    const LogicalPlan& plan,
    const std::function<LogicalOperator(const LogicalOperator& original, std::vector<LogicalOperator> rewrittenChildren)>& rewrite);

}

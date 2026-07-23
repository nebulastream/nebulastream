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
#include <PlanRewriteUtils.hpp>

#include <cstddef>
#include <deque>
#include <functional>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Schema/Field.hpp>

namespace NES
{
LogicalFunction replaceFieldAccesses(const LogicalFunction& function, const std::unordered_map<Field, Field>& fields)
{
    if (const auto fieldAccessFunction = function.tryGetAs<FieldAccessLogicalFunction>())
    {
        auto field = fieldAccessFunction.value()->getField();

        if (const auto it = fields.find(field); it != fields.end())
        {
            return FieldAccessLogicalFunction{it->second};
        }
        return function;
    }

    std::vector<LogicalFunction> newChildren;
    for (const auto& child : function.getChildren())
    {
        newChildren.emplace_back(replaceFieldAccesses(child, fields));
    }

    return function.withChildren(newChildren);
}

std::unordered_set<OperatorId> getSharedOperatorIds(const LogicalPlan& plan)
{
    /// Operator hashing/equality is structural, so visited-tracking must use operator ids.
    std::unordered_map<OperatorId, std::size_t> parentEdgeCounts;
    std::unordered_set<OperatorId> visited;
    std::deque<LogicalOperator> pending;
    for (const auto& root : plan.getRootOperators())
    {
        if (visited.insert(root.getId()).second)
        {
            pending.push_back(root);
        }
    }
    while (!pending.empty())
    {
        const auto current = pending.front();
        pending.pop_front();
        for (const auto& child : current.getChildren())
        {
            ++parentEdgeCounts[child.getId()];
            if (visited.insert(child.getId()).second)
            {
                pending.push_back(child);
            }
        }
    }

    std::unordered_set<OperatorId> sharedIds;
    for (const auto& [operatorId, parentEdges] : parentEdgeCounts)
    {
        if (parentEdges > 1)
        {
            sharedIds.insert(operatorId);
        }
    }
    return sharedIds;
}

namespace
{
LogicalOperator rewriteOperatorBottomUp(
    const LogicalOperator& original,
    const std::function<LogicalOperator(const LogicalOperator&, std::vector<LogicalOperator>)>& rewrite,
    std::unordered_map<OperatorId, LogicalOperator>& rewritten)
{
    if (const auto found = rewritten.find(original.getId()); found != rewritten.end())
    {
        return found->second;
    }

    std::vector<LogicalOperator> rewrittenChildren;
    for (const auto& child : original.getChildren())
    {
        rewrittenChildren.push_back(rewriteOperatorBottomUp(child, rewrite, rewritten));
    }

    auto result = rewrite(original, std::move(rewrittenChildren));
    rewritten.emplace(original.getId(), result);
    return result;
}
}

LogicalPlan rewritePlanBottomUp(
    const LogicalPlan& plan,
    const std::function<LogicalOperator(const LogicalOperator& original, std::vector<LogicalOperator> rewrittenChildren)>& rewrite)
{
    std::unordered_map<OperatorId, LogicalOperator> rewritten;
    std::vector<LogicalOperator> newRoots;
    newRoots.reserve(plan.getRootOperators().size());
    for (const auto& root : plan.getRootOperators())
    {
        newRoots.push_back(rewriteOperatorBottomUp(root, rewrite, rewritten));
    }
    return plan.withRootOperators(newRoots);
}
}

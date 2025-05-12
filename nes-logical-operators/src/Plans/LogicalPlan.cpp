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

#include <algorithm>
#include <cstddef>
#include <optional>
#include <ostream>
#include <set>
#include <sstream>
#include <stack>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/QueryConsoleDumpHandler.hpp>

namespace NES
{

QueryId LogicalPlan::getQueryId() const
{
    return queryId;
}

std::string LogicalPlan::getOriginalSql() const
{
    return originalSql;
}

void LogicalPlan::setOriginalSql(const std::string& sql)
{
    originalSql = sql;
}

void LogicalPlan::setQueryId(QueryId id)
{
    queryId = id;
}

std::vector<LogicalOperator> LogicalPlan::getRootOperators() const
{
    return rootOperators;
}

void LogicalPlan::setRootOperators(const std::vector<LogicalOperator>& operators)
{
    rootOperators = operators;
}

LogicalPlan::LogicalPlan(const LogicalPlan& other) : queryId(other.queryId), originalSql(other.originalSql)
{
    for (const auto& op : other.rootOperators)
    {
        rootOperators.push_back(op);
    }
}

LogicalPlan& LogicalPlan::operator=(const LogicalPlan& other)
{
    if (this != &other)
    {
        queryId = other.queryId;
        originalSql = other.originalSql;
        rootOperators.clear();
        for (const auto& op : other.rootOperators)
        {
            rootOperators.push_back(op);
        }
    }
    return *this;
}

LogicalPlan::LogicalPlan(LogicalPlan&& other) noexcept
    : queryId(other.queryId), rootOperators(std::move(other.rootOperators)), originalSql(std::move(other.originalSql))
{
}

LogicalPlan& LogicalPlan::operator=(LogicalPlan&& other) noexcept
{
    if (this != &other)
    {
        rootOperators = std::move(other.rootOperators);
        originalSql = std::move(other.originalSql);
        queryId = other.queryId;
    }
    return *this;
}

LogicalPlan::LogicalPlan(LogicalOperator rootOperator) : queryId(INVALID_QUERY_ID)
{
    rootOperators.push_back(std::move(rootOperator));
}

LogicalPlan::LogicalPlan(QueryId queryId, std::vector<LogicalOperator> rootOperators)
    : queryId(queryId), rootOperators(std::move(rootOperators))
{
}

LogicalPlan::LogicalPlan(const QueryId queryId, std::vector<LogicalOperator> rootOperators, std::string originalSql)
    : queryId(queryId), rootOperators(std::move(rootOperators)), originalSql(std::move(originalSql))
{
}

LogicalPlan promoteOperatorToRoot(const LogicalPlan& plan, const LogicalOperator& newRoot)
{
    auto root = newRoot.withChildren(plan.getRootOperators());
    return LogicalPlan(plan.getQueryId(), {std::move(root)});
}

namespace
{
bool replaceOperatorRecursion(LogicalOperator& current, const OperatorId target, LogicalOperator& replacement)
{
    if (current.getId() == target)
    {
        replacement = replacement.withChildren(current.getChildren());
        current = replacement;
        return true;
    }
    bool replaced = false;
    auto children = current.getChildren();
    for (auto& i : children)
    {
        if (replaceOperatorRecursion(i, target, replacement))
        {
            replaced = true;
        }
    }
    if (replaced)
    {
        current = current.withChildren(children);
    }
    return replaced;
}
}

std::optional<LogicalPlan> replaceOperator(const LogicalPlan& plan, const OperatorId target, LogicalOperator replacement)
{
    bool replaced = false;
    std::vector<LogicalOperator> newRoots;
    for (auto root : plan.getRootOperators())
    {
        if (root.getId() == target)
        {
            replacement = replacement.withChildren(root.getChildren());
            newRoots.push_back(replacement);
            replaced = true;
        }
        else if (replaceOperatorRecursion(root, target, replacement))
        {
            newRoots.push_back(root);
            replaced = true;
        }
    }
    if (replaced)
    {
        return LogicalPlan(plan.getQueryId(), std::move(newRoots), plan.getOriginalSql());
    }
    return std::nullopt;
}

namespace
{
bool replaceSubtreeRecursion(LogicalOperator& current, const OperatorId target, const LogicalOperator& replacement)
{
    if (current.getId() == target)
    {
        current = replacement;
        return true;
    }
    bool replaced = false;
    auto children = current.getChildren();
    for (auto& child : children)
    {
        if (replaceSubtreeRecursion(child, target, replacement))
        {
            replaced = true;
        }
    }
    if (replaced)
    {
        current = current.withChildren(std::move(children));
    }
    return replaced;
}
}

std::optional<LogicalPlan> replaceSubtree(const LogicalPlan& plan, const OperatorId target, const LogicalOperator& replacement)
{
    bool replaced = false;
    std::vector<LogicalOperator> newRoots = plan.getRootOperators();
    for (auto& root : newRoots)
    {
        if (root.getId() == target)
        {
            root = replacement;
            replaced = true;
        }
        else if (replaceSubtreeRecursion(root, target, replacement))
        {
            replaced = true;
        }
    }
    if (replaced)
    {
        return LogicalPlan(plan.getQueryId(), std::move(newRoots), plan.getOriginalSql());
    }
    return std::nullopt;
}

namespace
{
void getParentsHelper(const LogicalOperator& current, const LogicalOperator& target, std::vector<LogicalOperator>& parents)
{
    for (const auto& child : current.getChildren())
    {
        if (child.getId() == target.getId())
        {
            parents.push_back(current);
        }
        getParentsHelper(child, target, parents);
    }
}
}

std::vector<LogicalOperator> getParents(const LogicalPlan& plan, const LogicalOperator& target)
{
    std::vector<LogicalOperator> parents;
    for (const auto& root : plan.getRootOperators())
    {
        getParentsHelper(root, target, parents);
    }
    return parents;
}

std::string explain(const LogicalPlan& plan, ExplainVerbosity verbosity)
{
    std::stringstream stringstream;
    if (verbosity == ExplainVerbosity::Short)
    {
        auto dumpHandler = PlanRenderer<LogicalPlan, LogicalOperator>(stringstream, ExplainVerbosity::Short);
        dumpHandler.dump(plan);
    }
    else
    {
        auto dumpHandler = QueryConsoleDumpHandler<LogicalPlan, LogicalOperator>(stringstream, false);
        for (const auto& rootOperator : plan.getRootOperators())
        {
            dumpHandler.dump({rootOperator});
        }
    }
    return stringstream.str();
}

std::vector<LogicalOperator> getLeafOperators(const LogicalPlan& plan)
{
    /// Find all the leaf nodes in the query plan
    std::vector<LogicalOperator> leafOperators;
    /// Maintain a list of visited nodes as there are multiple root nodes
    std::set<OperatorId> visitedOpIds;
    for (const auto& rootOperator : plan.getRootOperators())
    {
        for (auto itr : BFSRange<LogicalOperator>(rootOperator))
        {
            if (visitedOpIds.contains(itr.getId()))
            {
                /// skip rest of the steps as the node found in already visited node list
                continue;
            }
            visitedOpIds.insert(itr.getId());
            if (itr.getChildren().empty())
            {
                leafOperators.push_back(itr);
            }
        }
    }
    return leafOperators;
}


std::optional<LogicalOperator> getOperatorById(const LogicalPlan& plan, OperatorId operatorId)
{
    for (const auto& rootOp : plan.getRootOperators())
    {
        for (const auto& op : BFSRange(rootOp))
        {
            if (op.getId() == operatorId)
            {
                return op;
            }
        }
    }
    return std::nullopt;
}

std::unordered_set<LogicalOperator> flatten(const LogicalPlan& plan)
{
    /// Maintain a list of visited nodes as there are multiple root nodes
    std::unordered_set<LogicalOperator> visitedOperators;
    for (const auto& rootOperator : plan.getRootOperators())
    {
        for (auto itr : BFSRange(rootOperator))
        {
            if (visitedOperators.contains(itr))
            {
                /// skip rest of the steps as the node found in already visited node list
                continue;
            }
            visitedOperators.insert(itr);
        }
    }
    return visitedOperators;
}

bool LogicalPlan::operator==(const LogicalPlan& other) const
{
    if (this == &other)
    {
        return true;
    }
    if (rootOperators.size() != other.rootOperators.size())
    {
        return false;
    }

    using Pair = std::pair<LogicalOperator, LogicalOperator>;
    std::stack<Pair> work;
    std::unordered_set<std::size_t> seenPairs; /// hash of (leftId<<32 | rightId)

    auto pushPair = [&](const LogicalOperator& l, const LogicalOperator& r)
    {
        const std::size_t key = ((l.getId().getRawValue()) << 32) | (r.getId().getRawValue());
        if (seenPairs.insert(key).second)
        {
            work.emplace(l, r);
        }
    };

    for (std::size_t i = 0; i < rootOperators.size(); ++i)
    {
        pushPair(rootOperators[i], other.rootOperators[i]);
    }

    while (!work.empty())
    {
        auto [l, r] = work.top();
        work.pop();

        if (l != r)
        {
            return false;
        }

        auto lc = l.getChildren();
        auto rc = r.getChildren();
        if (lc.size() != rc.size())
        {
            return false;
        }

        std::vector lcSorted(lc.begin(), lc.end());
        std::vector rcSorted(rc.begin(), rc.end());
        std::ranges::sort(lcSorted, [](auto& a, auto& b) { return a.getId() < b.getId(); });
        std::ranges::sort(rcSorted, [](auto& a, auto& b) { return a.getId() < b.getId(); });

        for (std::size_t i = 0; i < lcSorted.size(); ++i)
        {
            pushPair(lcSorted[i], rcSorted[i]);
        }
    }
    return true;
}

std::ostream& operator<<(std::ostream& os, const LogicalPlan& plan)
{
    return os << explain(plan, ExplainVerbosity::Short);
}

}

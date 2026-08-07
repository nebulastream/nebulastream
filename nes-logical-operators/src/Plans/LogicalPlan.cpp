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

#include <Plans/LogicalPlan.hpp>

#include <algorithm>
#include <cstddef>
#include <optional>
#include <ostream>
#include <sstream>
#include <stack>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include <Debug/DebugHelpers.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/QueryConsoleDumpHandler.hpp>

namespace NES
{

const QueryId& LogicalPlan::getQueryId() const
{
    return queryId;
}

void LogicalPlan::setQueryId(QueryId id)
{
    queryId = id;
}

std::string LogicalPlan::getOriginalSql() const
{
    return originalSql;
}

void LogicalPlan::setOriginalSql(const std::string& sql)
{
    originalSql = sql;
}

std::vector<LogicalOperator> LogicalPlan::getRootOperators() const
{
    return rootOperators;
}

LogicalPlan LogicalPlan::withRootOperators(const std::vector<LogicalOperator>& operators) const
{
    auto copy = *this;
    copy.rootOperators = operators;
    return copy;
}

LogicalPlan& LogicalPlan::operator=(const LogicalPlan& other)
{
    if (this != &other)
    {
        queryId = other.queryId;
        originalSql = other.originalSql;
        rootOperators = other.rootOperators;
    }
    return *this;
}

LogicalPlan::LogicalPlan(LogicalPlan&& other) noexcept
    : queryId(std::move(other.queryId)), rootOperators(std::move(other.rootOperators)), originalSql(std::move(other.originalSql))
{
}

LogicalPlan& LogicalPlan::operator=(LogicalPlan&& other) noexcept
{
    if (this != &other)
    {
        queryId = other.queryId;
        rootOperators = std::move(other.rootOperators);
        originalSql = std::move(other.originalSql);
    }
    return *this;
}

LogicalPlan::LogicalPlan(QueryId queryId, std::vector<LogicalOperator> rootOperators)
    : queryId(queryId), rootOperators(std::move(rootOperators))
{
}

LogicalPlan::LogicalPlan(QueryId queryId, std::vector<LogicalOperator> rootOperators, std::string originalSql)
    : queryId(queryId), rootOperators(std::move(rootOperators)), originalSql(std::move(originalSql))
{
}

LogicalPlan addRootOperators(const LogicalPlan& plan, const std::vector<LogicalOperator>& rootsToAdd)
{
    auto rootOps = plan.getRootOperators();
    rootOps.insert(rootOps.end(), rootsToAdd.begin(), rootsToAdd.end());
    return plan.withRootOperators(rootOps);
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

std::vector<LogicalOperator> planOperators(const LogicalPlan& plan)
{
    std::vector<LogicalOperator> operators;
    std::unordered_set<LogicalOperator> visited;
    for (const auto& root : plan.getRootOperators())
    {
        for (const auto& op : BFSRange(root))
        {
            if (visited.insert(op).second)
            {
                operators.push_back(op);
            }
        }
    }
    return operators;
}

namespace
{
/// Marks an operator an earlier sink already rendered; repeating its sub-plan would read as if each sink had a copy.
constexpr std::string_view SharedOperatorMarker = " [shared]";

void dumpOperator( /// NOLINT(misc-no-recursion)
    const LogicalOperator& op,
    const std::size_t level,
    std::ostream& out,
    const ExplainVerbosity verbosity,
    std::unordered_set<LogicalOperator>& alreadyDumped)
{
    const std::string indent(level * 2, ' ');
    if (not alreadyDumped.insert(op).second)
    {
        out << indent << op.explain(verbosity) << SharedOperatorMarker << '\n';
        return;
    }

    out << indent << op.explain(verbosity) << '\n';
    for (const auto& child : op.getChildren())
    {
        dumpOperator(child, level + 1, out, verbosity, alreadyDumped);
    }
}
}

std::string explain(const LogicalPlan& plan, ExplainVerbosity verbosity)
{
    std::stringstream stringstream;
    std::unordered_set<LogicalOperator> alreadyDumped;
    for (const auto& rootOperator : plan.getRootOperators())
    {
        dumpOperator(rootOperator, 0, stringstream, verbosity, alreadyDumped);
    }
    return stringstream.str();
}

std::vector<LogicalOperator> getLeafOperators(const LogicalPlan& plan)
{
    /// Find all the leaf nodes in the query plan
    std::vector<LogicalOperator> leafOperators;
    std::unordered_set<LogicalOperator> visited;
    for (const auto& rootOperator : plan.getRootOperators())
    {
        for (auto itr : BFSRange<LogicalOperator>(rootOperator))
        {
            if (not visited.insert(itr).second)
            {
                continue;
            }
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

std::unordered_set<LogicalOperator, StructuralOperatorHash, StructuralOperatorEqual> flatten(const LogicalPlan& plan)
{
    /// Maintain a list of visited nodes as there are multiple root nodes
    std::unordered_set<LogicalOperator, StructuralOperatorHash, StructuralOperatorEqual> visitedOperators;
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

        if (*l != *r)
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

namespace Debug
{

OperatorView view(const LogicalOperator& op) /// NOLINT(misc-no-recursion)
{
    OperatorView node;
    node.op = op.explain(ExplainVerbosity::Short);
    for (const auto& child : op.getChildren())
    {
        node.children.push_back(view(child));
    }
    return node;
}

PlanView view(const LogicalPlan& plan)
{
    PlanView result;
    std::ostringstream planLabel;
    planLabel << "LogicalPlan (queryId: " << plan.getQueryId() << ")";
    result.plan = planLabel.str();
    for (const auto& root : plan.getRootOperators())
    {
        result.roots.push_back(view(root));
    }
    return result;
}

}

}

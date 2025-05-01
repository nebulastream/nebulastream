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
#include <memory>
#include <set>
#include <stack>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/QueryConsoleDumpHandler.hpp>


namespace NES
{

LogicalPlan::LogicalPlan(const LogicalPlan& other) : queryId(other.queryId)
{
    for (const auto& op : other.rootOperators)
    {
        rootOperators.push_back(op);
    }
}

LogicalPlan::LogicalPlan(LogicalPlan&& other) : rootOperators(std::move(other.rootOperators)), queryId(other.queryId)
{
}

LogicalPlan& LogicalPlan::operator=(LogicalPlan&& other)
{
    if (this != &other)
    {
        rootOperators = std::move(other.rootOperators);
        queryId = other.queryId;
    }
    return *this;
}

LogicalPlan::LogicalPlan(LogicalOperator rootOperator) : queryId(INVALID_QUERY_ID)
{
    rootOperators.push_back(std::move(rootOperator));
}

LogicalPlan::LogicalPlan(QueryId queryId, std::vector<LogicalOperator> rootOperators)
    : rootOperators(std::move(rootOperators)), queryId(queryId)
{
}

void LogicalPlan::promoteOperatorToRoot(LogicalOperator newRoot)
{
    auto root = newRoot.withChildren(rootOperators);
    rootOperators.clear();
    rootOperators.push_back(root);
}

bool replaceOperatorHelper(LogicalOperator& current, const LogicalOperator& target, LogicalOperator replacement)
{
    if (current.getId() == target.getId())
    {
        replacement = replacement.withChildren(current.getChildren());
        current = replacement;
        return true;
    }
    bool replaced = false;
    auto children = current.getChildren();
    for (size_t i = 0; i < children.size(); ++i)
    {
        if (replaceOperatorHelper(children[i], target, replacement))
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

bool LogicalPlan::replaceOperator(const LogicalOperator& target, LogicalOperator replacement)
{
    bool replaced = false;
    for (auto& root : rootOperators)
    {
        if (root.getId() == target.getId())
        {
            replacement = replacement.withChildren(root.getChildren());
            root = replacement;

            replaced = true;
        }
        else if (replaceOperatorHelper(root, target, replacement))
        {
            replaced = true;
        }
    }
    return replaced;
}

bool replaceOperatorExactHelper(LogicalOperator& current,
                                             const LogicalOperator& target,
                                             const LogicalOperator& replacement)
{
    if (current.getId() == target.getId()) {
        current = replacement;
        return true;
    }
    bool replaced = false;
    auto children = current.getChildren();
    for (auto& child : children) {
        if (replaceOperatorExactHelper(child, target, replacement)) {
            replaced = true;
        }
    }
    if (replaced) {
        current = current.withChildren(std::move(children));
    }
    return replaced;
}


bool LogicalPlan::replaceOperatorExact(const LogicalOperator& target,
                                       LogicalOperator replacement)
{
    bool replaced = false;
    for (auto& root : rootOperators) {
        if (root.getId() == target.getId()) {
            root      = std::move(replacement);
            replaced  = true;
        } else if (replaceOperatorExactHelper(root, target, replacement)) {
            replaced = true;
        }
    }
    return replaced;
}


void getParentsHelper(const LogicalOperator& current, const LogicalOperator& target, std::vector<LogicalOperator>& parents)
{
    for (const auto& child : current.getChildren()) {
        if (child.getId() == target.getId()) {
            parents.push_back(current);
        }
        getParentsHelper(child, target, parents);
    }
}

std::vector<LogicalOperator> LogicalPlan::getParents(const LogicalOperator& target) const
{
    std::vector<LogicalOperator> parents;
    for (const auto& root : rootOperators) {
        getParentsHelper(root, target, parents);
    }
    return parents;
}


std::string LogicalPlan::toString() const
{
    std::stringstream ss;
    auto dumpHandler = QueryConsoleDumpHandler<LogicalPlan, LogicalOperator>(ss);
    for (auto& rootOperator : rootOperators)
    {
        dumpHandler.dump(rootOperator);
    }
    return ss.str();
}

std::vector<LogicalOperator> LogicalPlan::getLeafOperators() const
{
    /// Find all the leaf nodes in the query plan
    std::vector<LogicalOperator> leafOperators;
    /// Maintain a list of visited nodes as there are multiple root nodes
    std::set<OperatorId> visitedOpIds;
    for (const auto& rootOperator : rootOperators)
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

std::unordered_set<LogicalOperator> LogicalPlan::getAllOperators() const
{
    /// Maintain a list of visited nodes as there are multiple root nodes
    std::unordered_set<LogicalOperator> visitedOperators;
    for (const auto& rootOperator : rootOperators)
    {
        for (auto itr : BFSRange<LogicalOperator>(rootOperator))
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
    if (this == &other) {
        return true;
    }
    if (rootOperators.size() != other.rootOperators.size()) {
        return false;
    }

    using Pair = std::pair<LogicalOperator, LogicalOperator>;
    std::stack<Pair> work;
    std::unordered_set<std::size_t> seenPairs; /// hash of (leftId<<32 | rightId)

    auto pushPair = [&](const LogicalOperator& l, const LogicalOperator& r) {
        std::size_t key = ((l.getId().getRawValue()) << 32) |
            (r.getId().getRawValue());
        if (seenPairs.insert(key).second) {
            work.emplace(l, r);
        }
    };

    for (std::size_t i = 0; i < rootOperators.size(); ++i) {
        pushPair(rootOperators[i], other.rootOperators[i]);
    }

    while (!work.empty()) {
        auto [l, r] = work.top();
        work.pop();

        if (l != r) {
            return false;
        }

        auto lc = l.getChildren();
        auto rc = r.getChildren();
        if (lc.size() != rc.size()) {
            return false;
        }

        std::vector lcSorted(lc.begin(), lc.end());
        std::vector rcSorted(rc.begin(), rc.end());
        std::sort(lcSorted.begin(), lcSorted.end(),
                  [](auto& a, auto& b){ return a.getId() < b.getId(); });
        std::sort(rcSorted.begin(), rcSorted.end(),
                  [](auto& a, auto& b){ return a.getId() < b.getId(); });

        for (std::size_t i = 0; i < lcSorted.size(); ++i)
        {
            pushPair(lcSorted[i], rcSorted[i]);
        }
    }
    return true;
}

std::ostream& operator<<(std::ostream& os, const LogicalPlan& plan)
{
    return os <<  plan.toString();
}

}

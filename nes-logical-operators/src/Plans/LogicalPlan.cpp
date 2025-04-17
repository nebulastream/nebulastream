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

#include <deque>
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
    for (const auto& op : other.rootOperators) {
        rootOperators.push_back(op);
    }
}

LogicalPlan::LogicalPlan(LogicalPlan&& other)
    : rootOperators(std::move(other.rootOperators)), queryId(other.queryId)
{}

LogicalPlan& LogicalPlan::operator=(LogicalPlan&& other) {
    if (this != &other) {
        rootOperators = std::move(other.rootOperators);
        queryId = other.queryId;
    }
    return *this;
}

std::unique_ptr<LogicalPlan> LogicalPlan::create(LogicalOperator rootOperator)
{
    return std::make_unique<LogicalPlan>(std::move(rootOperator));
}

std::unique_ptr<LogicalPlan> LogicalPlan::create(QueryId queryId, std::vector<LogicalOperator> rootOperators)
{
    return std::make_unique<LogicalPlan>(std::move(queryId), std::move(rootOperators));
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

bool replaceOperatorHelper(LogicalOperator& current,
                        const LogicalOperator& target,
                        LogicalOperator replacement) {
    if (current.getId() == target.getId()) {
        replacement = replacement.withChildren(current.getChildren());
        current = replacement;
        return true;
    }
    bool replaced = false;
    auto children = current.getChildren();
    for (size_t i = 0; i < children.size(); ++i) {
        if (replaceOperatorHelper(children[i], target, replacement)) {
            replaced = true;
        }
    }
    if (replaced) {
        current = current.withChildren(children);
    }
    return replaced;
}

bool LogicalPlan::replaceOperator(const LogicalOperator& target, LogicalOperator replacement) {
    bool replaced = false;
    for (auto& root : rootOperators) {
        if (root.getId() == target.getId()) {
            replacement = replacement.withChildren(root.getChildren());
            root = replacement;

            replaced = true;
        } else if (replaceOperatorHelper(root, target, replacement)) {
            replaced = true;
        }
    }
    return replaced;
}

std::unique_ptr<LogicalPlan> LogicalPlan::flip() const
{
    std::unordered_map<OperatorId, std::vector<LogicalOperator>> reversedEdges;
    std::unordered_map<OperatorId, LogicalOperator> idToOperator;
    std::set<OperatorId> allOperators;

    for (const auto& root : rootOperators) {
        for (auto op : BFSRange<LogicalOperator>(root)) {
            idToOperator[op.getId()] = op;
            allOperators.insert(op.getId());
            for (const auto& child : op.getChildren()) {
                reversedEdges[child.getId()].push_back(op);
            }
        }
    }

    std::vector<LogicalOperator> newRoots;
    for (const auto& id : allOperators) {
        if (idToOperator[id].getChildren().empty()) {
            newRoots.push_back(idToOperator[id]);
        }
    }

    std::unordered_map<OperatorId, LogicalOperator> flippedOperators;
    std::unordered_set<OperatorId> visited;

    std::function<LogicalOperator(const LogicalOperator&)> flipOperator;
    flipOperator = [&](const LogicalOperator& op) -> LogicalOperator {
        auto opId = op.getId();
        if (visited.contains(opId)) {
            return flippedOperators[opId];
        }
        visited.insert(opId);

        LogicalOperator flippedOp = op;
        flippedOp = flippedOp.withChildren({});
        flippedOperators[opId] = flippedOp;

        std::vector<LogicalOperator> newChildren;
        for (const auto& parent : reversedEdges[opId]) {
            newChildren.push_back(flipOperator(parent));
        }
        return flippedOperators[opId].withChildren(newChildren);
    };

    std::vector<LogicalOperator> flippedRoots;
    std::unordered_set<OperatorId> rootIds;
    for (const auto& root : newRoots) {
        if (!rootIds.contains(root.getId())) {
            flippedRoots.push_back(flipOperator(root));
            rootIds.insert(root.getId());
        }
    }

    return std::make_unique<LogicalPlan>(getQueryId(), flippedRoots);
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
        for (auto itr :  BFSRange<LogicalOperator>(rootOperator))
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
        for (auto itr :  BFSRange<LogicalOperator>(rootOperator))
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

QueryId LogicalPlan::getQueryId() const
{
    return queryId;
}

void LogicalPlan::setQueryId(QueryId queryId)
{
    this->queryId = queryId;
}

bool LogicalPlan::operator==(const LogicalPlan& otherPlan) const
{
    auto leftRootOperators = this->rootOperators;
    auto rightRootOperators = otherPlan.rootOperators;

    if (leftRootOperators.size() != rightRootOperators.size())
    {
        return false;
    }

    /// add all root-operators to stack
    std::stack<std::pair<LogicalOperator, LogicalOperator>> stack;
    for (size_t i = 0; i < leftRootOperators.size(); ++i)
    {
        stack.emplace(
            leftRootOperators[i],
            rightRootOperators[i]
        );
    }

    /// iterate over stack
    while (!stack.empty())
    {
        auto [leftOperator, rightOperator] = stack.top();
        stack.pop();

        if (leftOperator != rightOperator)
        {
            return false;
        }

        if (leftOperator.getChildren().size() != rightOperator.getChildren().size())
            return false;

        /// discover children and add them to stack
        for (size_t j = 0; j < leftOperator.getChildren().size(); ++j)
        {
            auto leftChild = leftOperator.getChildren()[j];
            auto rightChild = rightOperator.getChildren()[j];
            stack.emplace(leftChild, rightChild);
        }
    }
    return true;
}

}

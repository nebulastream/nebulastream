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
#include <Plans/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/QueryConsoleDumpHandler.hpp>


namespace NES
{

QueryPlan::QueryPlan(const QueryPlan& other) : queryId(other.queryId)
{
    for (const auto& op : other.rootOperators) {
        rootOperators.push_back(op);
    }
}

QueryPlan::QueryPlan(QueryPlan&& other)
    : rootOperators(std::move(other.rootOperators)), queryId(other.queryId)
{}

QueryPlan& QueryPlan::operator=(QueryPlan&& other) {
    if (this != &other) {
        rootOperators = std::move(other.rootOperators);
        queryId = other.queryId;
    }
    return *this;
}

std::unique_ptr<QueryPlan> QueryPlan::create(LogicalOperator rootOperator)
{
    return std::make_unique<QueryPlan>(std::move(rootOperator));
}

std::unique_ptr<QueryPlan> QueryPlan::create(QueryId queryId, std::vector<LogicalOperator> rootOperators)
{
    return std::make_unique<QueryPlan>(std::move(queryId), std::move(rootOperators));
}

QueryPlan::QueryPlan(LogicalOperator rootOperator) : queryId(INVALID_QUERY_ID)
{
    rootOperators.push_back(std::move(rootOperator));
}

QueryPlan::QueryPlan(QueryId queryId, std::vector<LogicalOperator> rootOperators)
    : rootOperators(std::move(rootOperators)), queryId(queryId)
{
}

void QueryPlan::addRootOperator(LogicalOperator newRootOperator)
{
    /// Check if a root with the id already present
    auto found = std::find_if(
        rootOperators.begin(),
        rootOperators.end(),
        [&](const LogicalOperator& root) { return newRootOperator.getId() == root.getId(); });
    if (found == rootOperators.end())
    {
        rootOperators.push_back(std::move(newRootOperator));
    }
    else
    {
        NES_WARNING("Root operator with id {} already present in the plan. Will not add it to the roots.", newRootOperator.getId());
    }
}

void QueryPlan::promoteOperatorToRoot(LogicalOperator newRoot)
{
    for (auto& oldRoot : rootOperators)
    {
        //oldRoot->parents.emplace_back(newRoot);
        newRoot.setChildren({oldRoot});
    }
    rootOperators.clear();
    rootOperators.push_back(std::move(newRoot));
}

std::string QueryPlan::toString() const
{
    std::stringstream ss;
    auto dumpHandler = QueryConsoleDumpHandler::create(ss);
    for (auto& rootOperator : rootOperators)
    {
        dumpHandler->dump(rootOperator);
    }
    return ss.str();
}

std::vector<LogicalOperator> QueryPlan::getRootOperators() const
{
    std::vector<LogicalOperator> rawOps;
    rawOps.reserve(rootOperators.size());
    for (const auto& op : rootOperators) {
        rawOps.push_back(op);
    }
    return rawOps;
}

std::vector<LogicalOperator> QueryPlan::getLeafOperators() const
{
    /// Find all the leaf nodes in the query plan
    NES_DEBUG("QueryPlan: Get all leaf nodes in the query plan.");
    std::vector<LogicalOperator> leafOperators;
    /// Maintain a list of visited nodes as there are multiple root nodes
    std::set<OperatorId> visitedOpIds;
    NES_DEBUG("QueryPlan: Iterate over all root nodes to find the operator.");
    for (const auto& rootOperator : rootOperators)
    {
        for (auto itr :  BFSRange<LogicalOperator>(rootOperator))
        {
            if (visitedOpIds.contains(itr.getId()))
            {
                /// skip rest of the steps as the node found in already visited node list
                continue;
            }
            NES_DEBUG("QueryPlan: Inserting operator in collection of already visited node.");
            visitedOpIds.insert(itr.getId());
            if (itr.getChildren().empty())
            {
                NES_DEBUG("QueryPlan: Found leaf node. Adding to the collection of leaf nodes.");
                leafOperators.push_back(itr);
            }
        }
    }
    return leafOperators;
}

std::unordered_set<LogicalOperator> QueryPlan::getAllOperators() const
{
    /// Maintain a list of visited nodes as there are multiple root nodes
    std::unordered_set<LogicalOperator> visitedOperators;
    NES_DEBUG("QueryPlan: Iterate over all root nodes to find the operator.");
    for (const auto& rootOperator : rootOperators)
    {
        for (auto itr :  BFSRange<LogicalOperator>(rootOperator))
        {
            if (visitedOperators.contains(itr))
            {
                /// skip rest of the steps as the node found in already visited node list
                continue;
            }
            NES_DEBUG("QueryPlan: Inserting operator in collection of already visited node.");
            visitedOperators.insert(itr);
        }
    }
    return visitedOperators;
}

QueryId QueryPlan::getQueryId() const
{
    return queryId;
}

void QueryPlan::setQueryId(QueryId queryId)
{
    this->queryId = queryId;
}

bool QueryPlan::operator==(const QueryPlan& otherPlan) const
{
    auto leftRootOperators = this->getRootOperators();
    auto rightRootOperators = otherPlan.getRootOperators();

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
        auto [leftOperator, rightOperator] = operatorPairs.top();
        operatorPairs.pop();

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
            operatorPairs.emplace(leftChild, rightChild);
        }
    }
    return true;
}

}

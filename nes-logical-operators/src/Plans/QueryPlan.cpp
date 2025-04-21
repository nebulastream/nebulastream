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
#include <Plans/Operator.hpp>


namespace NES
{

QueryPlan::QueryPlan(const QueryPlan& other) : queryId(other.queryId)
{
    for (const auto& op : other.rootOperators) {
        if (op) {
            rootOperators.push_back(op->clone());
        }
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

std::unique_ptr<QueryPlan> QueryPlan::clone() const
{
    return std::make_unique<QueryPlan>(*this);
}

std::vector<std::unique_ptr<Operator>> QueryPlan::releaseRootOperators()
{
    return std::move(rootOperators);
}

std::unique_ptr<QueryPlan> QueryPlan::create(std::unique_ptr<Operator> rootOperator)
{
    return std::make_unique<QueryPlan>(std::move(rootOperator));
}

std::unique_ptr<QueryPlan> QueryPlan::create(QueryId queryId, std::vector<std::unique_ptr<Operator>> rootOperators)
{
    return std::make_unique<QueryPlan>(std::move(queryId), std::move(rootOperators));
}

QueryPlan::QueryPlan(std::unique_ptr<Operator> rootOperator) : queryId(INVALID_QUERY_ID)
{
    rootOperators.push_back(std::move(rootOperator));
}

QueryPlan::QueryPlan(QueryId queryId, std::vector<std::unique_ptr<Operator>> rootOperators)
    : rootOperators(std::move(rootOperators)), queryId(queryId)
{
}

void QueryPlan::addRootOperator(std::unique_ptr<Operator> newRootOperator)
{
    /// Check if a root with the id already present
    auto found = std::find_if(
        rootOperators.begin(),
        rootOperators.end(),
        [&](const std::unique_ptr<Operator>& root) { return newRootOperator->id == root->id; });
    if (found == rootOperators.end())
    {
        rootOperators.push_back(std::move(newRootOperator));
    }
    else
    {
        NES_WARNING("Root operator with id {} already present in the plan. Will not add it to the roots.", newRootOperator->id);
    }
}

void QueryPlan::promoteOperatorToRoot(std::unique_ptr<Operator> newRoot)
{
    for (auto& oldRoot : rootOperators)
    {
        oldRoot->parents.emplace_back(newRoot.get());
        newRoot->children.push_back(std::move(oldRoot));
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
        dumpHandler->dump(*rootOperator);
    }
    return ss.str();
}

std::vector<Operator*> QueryPlan::getRootOperators() const
{
    std::vector<Operator*> rawOps;
    rawOps.reserve(rootOperators.size());
    for (const auto& op : rootOperators) {
        rawOps.push_back(op.get());
    }
    return rawOps;
}

std::vector<Operator*> QueryPlan::getLeafOperators() const
{
    /// Find all the leaf nodes in the query plan
    NES_DEBUG("QueryPlan: Get all leaf nodes in the query plan.");
    std::vector<Operator*> leafOperators;
    /// Maintain a list of visited nodes as there are multiple root nodes
    std::set<OperatorId> visitedOpIds;
    NES_DEBUG("QueryPlan: Iterate over all root nodes to find the operator.");
    for (const auto& rootOperator : rootOperators)
    {
        for (auto itr :  BFSRange<Operator>(rootOperator.get()))
        {
            if (visitedOpIds.contains(itr->id))
            {
                /// skip rest of the steps as the node found in already visited node list
                continue;
            }
            NES_DEBUG("QueryPlan: Inserting operator in collection of already visited node.");
            visitedOpIds.insert(itr->id);
            if (itr->children.empty())
            {
                NES_DEBUG("QueryPlan: Found leaf node. Adding to the collection of leaf nodes.");
                leafOperators.push_back(itr);
            }
        }
    }
    return leafOperators;
}

std::unordered_set<Operator*> QueryPlan::getAllOperators() const
{
    /// Maintain a list of visited nodes as there are multiple root nodes
    std::unordered_set<Operator*> visitedOperators;
    NES_DEBUG("QueryPlan: Iterate over all root nodes to find the operator.");
    for (const auto& rootOperator : rootOperators)
    {
        for (auto itr :  BFSRange<Operator>(rootOperator.get()))
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
    std::stack<std::pair<Operator*, Operator*>> stack;
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
        /// get last discovered left and right operator
        auto [leftOperator, rightOperator] = stack.top();
        stack.pop();

        if (leftOperator->children.size() != rightOperator->children.size())
            return false;

        /// discover children and add them to stack
        for (size_t j = 0; j < leftOperator->children.size(); ++j)
        {
            auto leftChild = leftOperator->children[j].get();
            auto rightChild = rightOperator->children[j].get();
            if (!leftChild || !rightChild)
                return false;
            stack.emplace(leftChild, rightChild);
        }

        /// comparison of both operators
        if (leftOperator == rightOperator)
        {
            return false;
        }
    }
    return true;
}

}

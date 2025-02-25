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
#include <map>
#include <memory>
#include <set>
#include <stack>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Plans/QueryPlan.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/QueryConsoleDumpHandler.hpp>
#include <Plans/Operator.hpp>


namespace NES
{

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

std::vector<SinkLogicalOperator*> QueryPlan::getSinkOperators() const
{
    std::vector<SinkLogicalOperator*> sinkOperators;
    for (auto& rootOperator : rootOperators)
    {
        auto sinkOperator = dynamic_cast<SinkLogicalOperator*>(rootOperator.get());
        sinkOperators.emplace_back(sinkOperator);
    }
    NES_DEBUG("QueryPlan: Found {} sink operators.", sinkOperators.size());
    return sinkOperators;
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

const std::vector<Operator*> QueryPlan::getRootOperators() const
{
    std::vector<Operator*> rawPointers;
    rawPointers.reserve(rootOperators.size());
    for (const auto& op : rootOperators) {
        rawPointers.push_back(op.get());
    }
    return rawPointers;
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

std::set<Operator*> QueryPlan::findAllOperatorsBetween(
    const std::set<Operator*>& downstreamOperators,
    const std::set<Operator*>& upstreamOperators) const
{
    std::set<Operator*> result;
    std::set<Operator*> visited;

    for (Operator* startOp : downstreamOperators)
    {
        // Use a stack for DFS upward.
        std::vector<Operator*> stack;
        // Start from the parent of the downstream operator
        if (startOp->parents[0])
        {
            stack.push_back(startOp->parents[0]);
        }

        while (!stack.empty())
        {
            Operator* current = stack.back();
            stack.pop_back();

            // If we have already visited this operator, skip it.
            if (visited.find(current) != visited.end())
            {
                continue;
            }
            visited.insert(current);

            // If current is one of the upstream operators, do not include it,
            // and do not traverse further upward from here.
            if (upstreamOperators.find(current) != upstreamOperators.end())
            {
                continue;
            }

            // Otherwise, add the current operator to the result.
            result.insert(current);

            // Continue traversing upward if the parent exists.
            if (current->parents[0])
            {
                stack.push_back(current->parents[0]);
            }
        }
    }
    return result;
}


bool QueryPlan::operator==(const std::shared_ptr<QueryPlan>& otherPlan) const
{
    auto leftRootOperators = this->getRootOperators();
    auto rightRootOperators = otherPlan->getRootOperators();

    if (leftRootOperators.size() != rightRootOperators.size())
    {
        return false;
    }

    /// add all root-operators to stack
    std::stack<std::pair<std::shared_ptr<Operator>, std::shared_ptr<Operator>>> stack;
    for (size_t i = 0; i < leftRootOperators.size(); ++i)
    {
        stack.emplace(leftRootOperators[i], rightRootOperators[i]);
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
            auto& leftChild = leftOperator->children[j];
            auto& rightChild = rightOperator->children[j];
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

/*
std::set<std::shared_ptr<Operator>> QueryPlan::findOperatorsBetweenSourceAndTargetOperators(
    const std::shared_ptr<Operator>& sourceOperator, const std::set<std::shared_ptr<Operator>>& targetOperators)
{
    ///Find if downstream operator is also in the vector of target operators
    auto found = std::find_if(
        targetOperators.begin(),
        targetOperators.end(),
        [&](const auto& upstreamOperator) { return upstreamOperator->id == sourceOperator->id; });

    ///If downstream operator is in the list of target operators then return the operator
    if (found != targetOperators.end())
    {
        return {sourceOperator};
    }

    bool foundTargetUpstreamOperator = false;
    std::set<std::shared_ptr<Operator>> operatorsBetween;
    ///Check further upstream operators if they are in the target operator vector
    for (const auto& nextUpstreamOperatorToCheck : sourceOperator->children)
    {
        ///Fetch the operators between upstream and target operators
        auto operatorsBetweenUpstreamAndTargetUpstream
            = findOperatorsBetweenSourceAndTargetOperators(NES::Util::as_if<Operator>(nextUpstreamOperatorToCheck), targetOperators);

        ///If there are operators between upstream and target operators then mark the input down stream operator for return
        if (!operatorsBetweenUpstreamAndTargetUpstream.empty())
        {
            foundTargetUpstreamOperator = true;
            operatorsBetween.insert(operatorsBetweenUpstreamAndTargetUpstream.begin(), operatorsBetweenUpstreamAndTargetUpstream.end());
        }
    }

    ///Add downstream operator
    if (foundTargetUpstreamOperator)
    {
        operatorsBetween.insert(sourceOperator);
    }
    return operatorsBetween;
}
*/
}

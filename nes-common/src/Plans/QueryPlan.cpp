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

std::shared_ptr<QueryPlan> QueryPlan::create(std::shared_ptr<Operator> rootOperator)
{
    return std::make_shared<QueryPlan>(std::move(rootOperator));
}

std::shared_ptr<QueryPlan> QueryPlan::create(QueryId queryId, std::vector<std::shared_ptr<Operator>> rootOperators)
{
    return std::make_shared<QueryPlan>(std::move(queryId), std::move(rootOperators));
}

QueryPlan::QueryPlan(std::shared_ptr<Operator> rootOperator) : queryId(INVALID_QUERY_ID)
{
    rootOperators.push_back(std::move(rootOperator));
}

QueryPlan::QueryPlan(QueryId queryId, std::vector<std::shared_ptr<Operator>> rootOperators)
    : rootOperators(std::move(rootOperators)), queryId(queryId)
{
}

std::vector<std::shared_ptr<SinkLogicalOperator>> QueryPlan::getSinkOperators() const
{
    NES_DEBUG("QueryPlan: Get all sink operators by traversing all the root nodes.");
    std::vector<std::shared_ptr<SinkLogicalOperator>> sinkOperators;
    for (const auto& rootOperator : rootOperators)
    {
        auto sinkOperator = NES::Util::as<SinkLogicalOperator>(rootOperator);
        sinkOperators.emplace_back(sinkOperator);
    }
    NES_DEBUG("QueryPlan: Found {} sink operators.", sinkOperators.size());
    return sinkOperators;
}

std::string QueryPlan::toString() const
{
    std::stringstream ss;
    auto dumpHandler = QueryConsoleDumpHandler::create(ss);
    for (const auto& rootOperator : rootOperators)
    {
        dumpHandler->dump(rootOperator);
    }
    return ss.str();
}

std::vector<std::shared_ptr<Operator>> QueryPlan::getRootOperators() const
{
    return rootOperators;
}

std::vector<std::shared_ptr<Operator>> QueryPlan::getLeafOperators() const
{
    /// Find all the leaf nodes in the query plan
    NES_DEBUG("QueryPlan: Get all leaf nodes in the query plan.");
    std::vector<std::shared_ptr<Operator>> leafOperators;
    /// Maintain a list of visited nodes as there are multiple root nodes
    std::set<OperatorId> visitedOpIds;
    NES_DEBUG("QueryPlan: Iterate over all root nodes to find the operator.");
    for (const auto& rootOperator : rootOperators)
    {
        for (auto itr :  BFSRange<Operator>(rootOperator))
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

std::unordered_set<std::shared_ptr<Operator>> QueryPlan::getAllOperators() const
{
    /// Maintain a list of visited nodes as there are multiple root nodes
    std::unordered_set<std::shared_ptr<Operator>> visitedOperators;
    NES_DEBUG("QueryPlan: Iterate over all root nodes to find the operator.");
    for (const auto& rootOperator : rootOperators)
    {
        for (auto itr :  BFSRange<Operator>(rootOperator))
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

void QueryPlan::appendOperatorAsNewRoot(const std::shared_ptr<Operator>& operatorNode)
{
    for (const auto& rootOperator : rootOperators)
    {
        const auto result = rootOperator->parents.emplace_back(operatorNode);
        PRECONDITION(result, "QueryPlan: Unable to add operator {0} as parent to {0}", *operatorNode);
    }
    rootOperators.clear();
    rootOperators.push_back(operatorNode);
}

std::set<std::shared_ptr<Operator>> QueryPlan::findAllOperatorsBetween(
    const std::set<std::shared_ptr<Operator>>& downstreamOperators, const std::set<std::shared_ptr<Operator>>& upstreamOperators)
{
    std::set<std::shared_ptr<Operator>> operatorsBetween;

    ///initialize the operators to visit with upstream operators of all downstream operators
    for (const auto& downStreamOperator : downstreamOperators)
    {
        auto operatorsBetweenChildAndTargetUpstream
            = findOperatorsBetweenSourceAndTargetOperators(NES::Util::as_if<Operator>(downStreamOperator), upstreamOperators);
        operatorsBetween.insert(operatorsBetweenChildAndTargetUpstream.begin(), operatorsBetweenChildAndTargetUpstream.end());
    }

    if (!operatorsBetween.empty())
    {
        ///Remove upstream operators
        for (const auto& upstreamOperator : upstreamOperators)
        {
            erase_if(
                operatorsBetween,
                [upstreamOperator](const auto& operatorToErase) { return operatorToErase->id == upstreamOperator->id; });
        }

        ///Remove downstream operators
        for (const auto& downstreamOperator : downstreamOperators)
        {
            erase_if(
                operatorsBetween,
                [downstreamOperator](const auto& operatorToErase) { return operatorToErase->id == downstreamOperator->id; });
        }
    }

    return operatorsBetween;
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

        auto leftChildren = leftOperator->children;
        auto rightChildren = rightOperator->children;

        if (leftChildren.size() != rightChildren.size())
            return false;

        /// discover children and add them to stack
        for (size_t j = 0; j < leftChildren.size(); ++j)
        {
            auto leftChild = NES::Util::as<Operator>(leftChildren[j]);
            auto rightChild = NES::Util::as<Operator>(rightChildren[j]);
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

}

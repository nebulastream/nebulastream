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
#include <deque>
#include <memory>
#include <set>
#include <stack>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Nodes/Iterators/BreadthFirstNodeIterator.hpp>
#include <Nodes/Node.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceNameLogicalOperator.hpp>
#include <Operators/Operator.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/LogicalQueryDumpHelper.hpp>

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

void QueryPlan::appendOperatorAsNewRoot(const std::shared_ptr<Operator>& operatorNode)
{
    NES_DEBUG("QueryPlan: Appending operator {} as new root of the plan.", *operatorNode);
    for (const auto& rootOperator : rootOperators)
    {
        const auto result = rootOperator->addParent(operatorNode);
        PRECONDITION(result, "QueryPlan: Unable to add operator {} as parent to {}", *operatorNode, *operatorNode);
    }
    NES_DEBUG("QueryPlan: Clearing current root operators.");
    clearRootOperators();
    NES_DEBUG("QueryPlan: Pushing input operator node as new root.");
    rootOperators.push_back(operatorNode);
}

void QueryPlan::clearRootOperators()
{
    rootOperators.clear();
}

std::string QueryPlan::toString() const
{
    std::stringstream ss;
    auto dumpHandler = LogicalQueryDumpHelper(ss);
    dumpHandler.dump(*this);
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
        auto bfsIterator = BreadthFirstNodeIterator(rootOperator);
        for (auto itr = bfsIterator.begin(); itr != NES::BreadthFirstNodeIterator::end(); ++itr)
        {
            auto visitingOp = NES::Util::as<Operator>(*itr);
            if (visitedOpIds.contains(visitingOp->getId()))
            {
                /// skip rest of the steps as the node found in already visited node list
                continue;
            }
            NES_DEBUG("QueryPlan: Inserting operator in collection of already visited node.");
            visitedOpIds.insert(visitingOp->getId());
            if (visitingOp->getChildren().empty())
            {
                NES_DEBUG("QueryPlan: Found leaf node. Adding to the collection of leaf nodes.");
                leafOperators.push_back(visitingOp);
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
        auto bfsIterator = BreadthFirstNodeIterator(rootOperator);
        for (auto itr = bfsIterator.begin(); itr != BreadthFirstNodeIterator::end(); ++itr)
        {
            auto visitingOp = NES::Util::as<Operator>(*itr);
            if (visitedOperators.contains(visitingOp))
            {
                /// skip rest of the steps as the node found in already visited node list
                continue;
            }
            NES_DEBUG("QueryPlan: Inserting operator in collection of already visited node.");
            visitedOperators.insert(visitingOp);
        }
    }
    return visitedOperators;
}

bool QueryPlan::hasOperatorWithId(OperatorId operatorId) const
{
    NES_DEBUG("QueryPlan: Checking if the operator exists in the query plan or not");
    if (getOperatorWithOperatorId(operatorId))
    {
        return true;
    }
    NES_DEBUG("QueryPlan: Unable to find operator with matching Id");
    return false;
}

std::shared_ptr<Operator> QueryPlan::getOperatorWithOperatorId(OperatorId operatorId) const
{
    NES_DEBUG("QueryPlan: Checking if the operator with id {} exists in the query plan or not", operatorId);
    for (auto rootOperator : rootOperators)
    {
        if (rootOperator->getId() == operatorId)
        {
            NES_DEBUG("QueryPlan: Found operator {} in the query plan", operatorId);
            return rootOperator;
        }

        ///Look up in the child operators
        auto matchingOperator = rootOperator->getChildWithOperatorId(operatorId);
        if (matchingOperator)
        {
            return NES::Util::as<Operator>(matchingOperator);
        }
    }
    NES_DEBUG("QueryPlan: Unable to find operator with matching Id");
    return nullptr;
}

QueryId QueryPlan::getQueryId() const
{
    return queryId;
}

void QueryPlan::setQueryId(QueryId queryId)
{
    this->queryId = queryId;
}

void QueryPlan::addRootOperator(const std::shared_ptr<Operator>& newRootOperator)
{
    ///Check if a root with the id already present
    auto found = std::ranges::find_if(
        rootOperators, [&](const std::shared_ptr<Operator>& root) { return newRootOperator->getId() == root->getId(); });

    /// If not present then add it
    if (found == rootOperators.end())
    {
        rootOperators.push_back(newRootOperator);
    }
    else
    {
        NES_WARNING("Root operator with id {} already present int he plan", newRootOperator->getId());
    }
}

void QueryPlan::removeAsRootOperator(std::shared_ptr<Operator> root)
{
    NES_DEBUG("QueryPlan: removing operator {} as root operator.", *root);
    auto found = std::ranges::find_if(
        rootOperators, [&](const std::shared_ptr<Operator>& rootOperator) { return rootOperator->getId() == root->getId(); });
    if (found != rootOperators.end())
    {
        NES_DEBUG(
            "QueryPlan: Found root operator in the root operator list. Removing the operator as the root of the query plan. {}", *root);
        rootOperators.erase(found);
    }
}

std::shared_ptr<QueryPlan> QueryPlan::copy()
{
    NES_INFO("QueryPlan: make copy of this query plan");
    /// 1. We start by copying the root operators of this query plan to the queue of operators to be processed
    std::map<OperatorId, std::shared_ptr<Operator>> operatorIdToOperatorMap;
    std::deque<std::shared_ptr<Node>> operatorsToProcess{rootOperators.begin(), rootOperators.end()};
    while (!operatorsToProcess.empty())
    {
        auto operatorNode = NES::Util::as<Operator>(operatorsToProcess.front());
        operatorsToProcess.pop_front();
        auto operatorId = operatorNode->getId();
        /// 2. We add each non existing operator to a map and skip adding the operator that already exists in the map.
        /// 3. We use the already existing operator whenever available other wise we create a copy of the operator and add it to the map.
        if (operatorIdToOperatorMap[operatorId])
        {
            NES_TRACE("QueryPlan: Operator was processed previously");
            operatorNode = operatorIdToOperatorMap[operatorId];
        }
        else
        {
            NES_TRACE("QueryPlan: Adding the operator into map");
            operatorIdToOperatorMap[operatorId] = operatorNode;
        }

        /// 4. We then check the parent operators of the current operator by looking into the map and add them as the parent of the current operator.
        for (const auto& parentNode : operatorNode->getParents())
        {
            auto parentOperator = NES::Util::as<Operator>(parentNode);
            auto parentOperatorId = parentOperator->getId();
            if (operatorIdToOperatorMap[parentOperatorId])
            {
                NES_TRACE("QueryPlan: Found the parent operator. Adding as parent to the current operator.");
                parentOperator = operatorIdToOperatorMap[parentOperatorId];
                auto copyOfOperator = operatorIdToOperatorMap[operatorNode->getId()];
                copyOfOperator->addParent(parentOperator);
            }
            else
            {
                NES_ERROR("QueryPlan: unable to find the parent operator. This should not have occurred!");
                return nullptr;
            }
        }

        NES_TRACE("QueryPlan: add the child global query nodes for further processing.");
        /// 5. We push the children operators to the queue of operators to be processed.
        for (const auto& childrenOperator : operatorNode->getChildren())
        {
            operatorsToProcess.push_back(childrenOperator);
        }
    }

    std::vector<std::shared_ptr<Operator>> duplicateRootOperators;
    for (const auto& rootOperator : rootOperators)
    {
        NES_TRACE("QueryPlan: Finding the operator with same id in the map.");
        duplicateRootOperators.push_back(operatorIdToOperatorMap[rootOperator->getId()]);
    }
    operatorIdToOperatorMap.clear();
    auto newQueryPlan = QueryPlan::create(queryId, duplicateRootOperators);
    newQueryPlan->setSourceConsumed(sourceConsumed);
    newQueryPlan->setPlacementStrategy(placementStrategy);
    newQueryPlan->setQueryState(currentState);
    return newQueryPlan;
}

std::string QueryPlan::getSourceConsumed() const
{
    return sourceConsumed;
}

void QueryPlan::setSourceConsumed(std::string_view sourceName)
{
    sourceConsumed = sourceName;
}

Optimizer::PlacementStrategy QueryPlan::getPlacementStrategy() const
{
    return placementStrategy;
}

void QueryPlan::setPlacementStrategy(Optimizer::PlacementStrategy placementStrategy)
{
    this->placementStrategy = placementStrategy;
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
                [upstreamOperator](const auto& operatorToErase) { return operatorToErase->getId() == upstreamOperator->getId(); });
        }

        ///Remove downstream operators
        for (const auto& downstreamOperator : downstreamOperators)
        {
            erase_if(
                operatorsBetween,
                [downstreamOperator](const auto& operatorToErase) { return operatorToErase->getId() == downstreamOperator->getId(); });
        }
    }

    return operatorsBetween;
}

bool QueryPlan::compare(const std::shared_ptr<QueryPlan>& otherPlan) const
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

        auto leftChildren = leftOperator->getChildren();
        auto rightChildren = rightOperator->getChildren();

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
        if (!leftOperator->equal(rightOperator))
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
    auto found = std::ranges::find_if(
        targetOperators, [&](const auto& upstreamOperator) { return upstreamOperator->getId() == sourceOperator->getId(); });

    ///If downstream operator is in the list of target operators then return the operator
    if (found != targetOperators.end())
    {
        return {sourceOperator};
    }

    bool foundTargetUpstreamOperator = false;
    std::set<std::shared_ptr<Operator>> operatorsBetween;
    ///Check further upstream operators if they are in the target operator vector
    for (const auto& nextUpstreamOperatorToCheck : sourceOperator->getChildren())
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

QueryState QueryPlan::getQueryState() const
{
    return currentState;
}

void QueryPlan::setQueryState(QueryState newState)
{
    currentState = newState;
}
}

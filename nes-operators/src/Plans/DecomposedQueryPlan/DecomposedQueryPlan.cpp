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
#include <queue>
#include <unordered_set>
#include <Nodes/Iterators/BreadthFirstNodeIterator.hpp>
#include <Nodes/Node.hpp>
#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceNameLogicalOperator.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/LogicalQueryDumpHelper.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

DecomposedQueryPlan::DecomposedQueryPlan(QueryId queryId, WorkerId workerId) : queryId(queryId), workerId(workerId)
{
}

DecomposedQueryPlan::DecomposedQueryPlan(QueryId queryId, WorkerId workerId, std::vector<std::shared_ptr<Operator>> rootOperators)
    : queryId(queryId), workerId(workerId), rootOperators(std::move(rootOperators))
{
}

void DecomposedQueryPlan::addRootOperator(std::shared_ptr<Operator> newRootOperator)
{
    rootOperators.emplace_back(newRootOperator);
}

bool DecomposedQueryPlan::removeAsRootOperator(OperatorId rootOperatorId)
{
    NES_WARNING("Remove root operator with id {}", rootOperatorId);
    auto found = std::ranges::find_if(rootOperators, [&](const auto& rootOperator) { return rootOperator->getId() == rootOperatorId; });

    if (found == rootOperators.end())
    {
        NES_WARNING("Unable to locate a root operator with id {}. Skipping remove root operator operation.", rootOperatorId);
        return false;
    }

    rootOperators.erase(found);
    return true;
}

std::vector<std::shared_ptr<Operator>> DecomposedQueryPlan::getRootOperators() const
{
    return rootOperators;
}

std::vector<std::shared_ptr<Operator>> DecomposedQueryPlan::getLeafOperators() const
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
        for (auto itr = bfsIterator.begin(); itr != BreadthFirstNodeIterator::end(); ++itr)
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

QueryId DecomposedQueryPlan::getQueryId() const
{
    return queryId;
}

void DecomposedQueryPlan::setQueryId(QueryId queryId)
{
    this->queryId = queryId;
}

std::vector<std::shared_ptr<SinkLogicalOperator>> DecomposedQueryPlan::getSinkOperators() const
{
    NES_DEBUG("Get all sink operators by traversing all the root nodes.");
    std::vector<std::shared_ptr<SinkLogicalOperator>> sinkOperators;
    for (const auto& rootOperator : rootOperators)
    {
        auto sinkOperator = NES::Util::as<SinkLogicalOperator>(rootOperator);
        sinkOperators.emplace_back(sinkOperator);
    }
    NES_DEBUG("Found {} sink operators.", sinkOperators.size());
    return sinkOperators;
}

bool DecomposedQueryPlan::hasOperatorWithId(OperatorId operatorId) const
{
    NES_DEBUG("Checking if the operator exists in the query plan or not");
    if (getOperatorWithOperatorId(operatorId))
    {
        return true;
    }
    NES_DEBUG("QueryPlan: Unable to find operator with matching Id");
    return false;
}

std::shared_ptr<Operator> DecomposedQueryPlan::getOperatorWithOperatorId(OperatorId operatorId) const
{
    NES_DEBUG("Checking if the operator with id {} exists in the query plan or not", operatorId);
    for (auto rootOperator : rootOperators)
    {
        if (rootOperator->getId() == operatorId)
        {
            NES_DEBUG("Found operator {} in the query plan", operatorId);
            return rootOperator;
        }

        ///Look up in the child operators
        auto matchedOperator = rootOperator->getChildWithOperatorId(operatorId);
        if (matchedOperator)
        {
            return NES::Util::as<LogicalOperator>(matchedOperator);
        }
    }
    NES_DEBUG("Unable to find operator with matching Id");
    return nullptr;
}

bool DecomposedQueryPlan::replaceRootOperator(const std::shared_ptr<Operator>& oldRoot, const std::shared_ptr<Operator>& newRoot)
{
    for (auto& rootOperator : rootOperators)
    {
        /// compares the pointers and checks if we found the correct operator.
        if (rootOperator == oldRoot)
        {
            rootOperator = newRoot;
            return true;
        }
    }
    return false;
}

void DecomposedQueryPlan::appendOperatorAsNewRoot(const std::shared_ptr<Operator>& operatorNode)
{
    NES_DEBUG("QueryPlan: Appending operator {} as new root of the plan.", *operatorNode);
    for (const auto& rootOperator : rootOperators)
    {
        const auto result = rootOperator->addParent(operatorNode);
        INVARIANT(result, "QueryPlan: Unable to add operator {} as parent to {}", *operatorNode, *rootOperator);
    }
    NES_DEBUG("QueryPlan: Clearing current root operators.");
    rootOperators.clear();
    NES_DEBUG("QueryPlan: Pushing input operator node as new root.");
    rootOperators.push_back(operatorNode);
}

std::unordered_set<std::shared_ptr<Operator>> DecomposedQueryPlan::getAllOperators() const
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

std::shared_ptr<DecomposedQueryPlan> DecomposedQueryPlan::copy() const
{
    NES_INFO("DecomposedQueryPlan: make copy.");
    /// 1. We start by copying the root operators of this query plan to the queue of operators to be processed
    std::map<OperatorId, std::shared_ptr<Operator>> operatorIdToOperatorMap;
    std::deque<std::shared_ptr<Node>> operatorsToProcess{rootOperators.begin(), rootOperators.end()};
    while (!operatorsToProcess.empty())
    {
        auto operatorNode = NES::Util::as<Operator>(operatorsToProcess.front());
        operatorsToProcess.pop_front();
        OperatorId operatorId = operatorNode->getId();
        /// 2. We add each non existing operator to a map and skip adding the operator that already exists in the map.
        /// 3. We use the already existing operator whenever available other wise we create a copy of the operator and add it to the map.
        if (operatorIdToOperatorMap[operatorId])
        {
            NES_TRACE("DecomposedQueryPlan: Operator was processed previously");
            operatorNode = operatorIdToOperatorMap[operatorId];
        }
        else
        {
            NES_TRACE("DecomposedQueryPlan: Adding the operator into map");
            operatorIdToOperatorMap[operatorId] = operatorNode->copy();
        }

        /// 4. We then check the parent operators of the current operator by looking into the map and add them as the parent of the current operator.
        for (const auto& parentNode : operatorNode->getParents())
        {
            auto parentOperator = NES::Util::as<Operator>(parentNode);
            OperatorId parentOperatorId = parentOperator->getId();
            if (operatorIdToOperatorMap.contains(parentOperatorId))
            {
                NES_TRACE("DecomposedQueryPlan: Found the parent operator. Adding as parent to the current operator.");
                parentOperator = operatorIdToOperatorMap[parentOperatorId];
                auto copyOfOperator = operatorIdToOperatorMap[operatorNode->getId()];
                copyOfOperator->addParent(parentOperator);
            }
            else
            {
                INVARIANT(false, "DecomposedQueryPlan: Copying the plan failed because parent operator not found.");
            }
        }

        NES_TRACE("DecomposedQueryPlan: add the child global query nodes for further processing.");
        /// 5. We push the children operators to the queue of operators to be processed.
        for (const auto& childrenOperator : operatorNode->getChildren())
        {
            ///Check if all parents were processed
            auto parentOperators = childrenOperator->getParents();
            bool processedAllParent = true;
            for (const auto& parentOperator : parentOperators)
            {
                if (!operatorIdToOperatorMap.contains(NES::Util::as<Operator>(parentOperator)->getId()))
                {
                    processedAllParent = false;
                    break;
                }
            }

            /// Add child only if all parents were processed
            if (processedAllParent)
            {
                operatorsToProcess.push_back(childrenOperator);
            }
        }
    }

    std::vector<std::shared_ptr<Operator>> duplicateRootOperators;
    for (const auto& rootOperator : rootOperators)
    {
        NES_TRACE("DecomposedQueryPlan: Finding the operator with same id in the map.");
        duplicateRootOperators.push_back(operatorIdToOperatorMap[rootOperator->getId()]);
    }
    operatorIdToOperatorMap.clear();

    /// Create the duplicated decomposed query plan
    auto copiedDecomposedQueryPlan = std::make_shared<DecomposedQueryPlan>(queryId, workerId, duplicateRootOperators);
    return copiedDecomposedQueryPlan;
}

std::string DecomposedQueryPlan::toString() const
{
    std::stringstream ss;
    auto dumpHandler = LogicalQueryDumpHelper(ss);
    for (const auto& rootOperator : rootOperators)
    {
        dumpHandler.dump({rootOperator});
    }
    return ss.str();
}

WorkerId DecomposedQueryPlan::getWorkerId() const
{
    return workerId;
}

}

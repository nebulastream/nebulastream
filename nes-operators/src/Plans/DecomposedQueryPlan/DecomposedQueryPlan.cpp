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

#include <Nodes/Iterators/BreadthFirstNodeIterator.hpp>
#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/QueryConsoleDumpHandler.hpp>
#include <queue>

namespace NES {

DecomposedQueryPlanPtr DecomposedQueryPlan::create(DecomposedQueryPlanId decomposedQueryPlanId, SharedQueryId sharedQueryId) {
    return std::make_shared<DecomposedQueryPlan>(decomposedQueryPlanId, sharedQueryId);
}

DecomposedQueryPlan::DecomposedQueryPlan(DecomposedQueryPlanId decomposedQueryPlanId, SharedQueryId sharedQueryId)
    : decomposedQueryPlanId(decomposedQueryPlanId), sharedQueryId(sharedQueryId),
      currentState(QueryState::MARKED_FOR_DEPLOYMENT) {}

void DecomposedQueryPlan::addRootOperator(OperatorNodePtr newRootOperator) { rootOperators.emplace_back(newRootOperator); }

bool DecomposedQueryPlan::removeAsRootOperator(OperatorId rootOperatorId) {
    NES_WARNING("Remove root operator with id {}", rootOperatorId);
    auto found = std::find_if(rootOperators.begin(), rootOperators.end(), [&](const auto& rootOperator) {
        return rootOperator->getId() == rootOperatorId;
    });

    if (found == rootOperators.end()) {
        NES_WARNING("Unable to locate a root operator with id {}. Skipping remove root operator operation.", rootOperatorId);
        return false;
    }

    rootOperators.erase(found);
    return true;
}

std::vector<OperatorNodePtr> DecomposedQueryPlan::getRootOperators() { return rootOperators; }

std::vector<OperatorNodePtr> DecomposedQueryPlan::getLeafOperators() {
    // Find all the leaf nodes in the query plan
    NES_DEBUG("QueryPlan: Get all leaf nodes in the query plan.");
    std::vector<OperatorNodePtr> leafOperators;
    // Maintain a list of visited nodes as there are multiple root nodes
    std::set<OperatorId> visitedOpIds;
    NES_DEBUG("QueryPlan: Iterate over all root nodes to find the operator.");
    for (const auto& rootOperator : rootOperators) {
        auto bfsIterator = BreadthFirstNodeIterator(rootOperator);
        for (auto itr = bfsIterator.begin(); itr != BreadthFirstNodeIterator::end(); ++itr) {
            auto visitingOp = (*itr)->as<OperatorNode>();
            if (visitedOpIds.contains(visitingOp->getId())) {
                // skip rest of the steps as the node found in already visited node list
                continue;
            }
            NES_DEBUG("QueryPlan: Inserting operator in collection of already visited node.");
            visitedOpIds.insert(visitingOp->getId());
            if (visitingOp->getChildren().empty()) {
                NES_DEBUG("QueryPlan: Found leaf node. Adding to the collection of leaf nodes.");
                leafOperators.push_back(visitingOp);
            }
        }
    }
    return leafOperators;
}

DecomposedQueryPlanId DecomposedQueryPlan::getDecomposedQueryPlanId() { return decomposedQueryPlanId; }

SharedQueryId DecomposedQueryPlan::getSharedQueryId() { return sharedQueryId; }

void DecomposedQueryPlan::setDecomposedQueryPlanId(DecomposedQueryPlanId newDecomposedQueryPlanId) {
    decomposedQueryPlanId = newDecomposedQueryPlanId;
}

std::vector<SourceLogicalOperatorNodePtr> DecomposedQueryPlan::getSourceOperators() {
    NES_DEBUG("Get all source operators by traversing all the root nodes.");
    std::set<SourceLogicalOperatorNodePtr> sourceOperatorsSet;
    for (const auto& rootOperator : rootOperators) {
        auto sourceOperators = rootOperator->getNodesByType<SourceLogicalOperatorNode>();
        NES_DEBUG("Insert all source operators to the collection");
        sourceOperatorsSet.insert(sourceOperators.begin(), sourceOperators.end());
    }
    NES_DEBUG("Found {} source operators.", sourceOperatorsSet.size());
    std::vector<SourceLogicalOperatorNodePtr> sourceOperators{sourceOperatorsSet.begin(), sourceOperatorsSet.end()};
    return sourceOperators;
}

std::vector<SinkLogicalOperatorNodePtr> DecomposedQueryPlan::getSinkOperators() {
    NES_DEBUG("Get all sink operators by traversing all the root nodes.");
    std::vector<SinkLogicalOperatorNodePtr> sinkOperators;
    for (const auto& rootOperator : rootOperators) {
        auto sinkOperator = rootOperator->as<SinkLogicalOperatorNode>();
        sinkOperators.emplace_back(sinkOperator);
    }
    NES_DEBUG("Found {} sink operators.", sinkOperators.size());
    return sinkOperators;
}

QueryState DecomposedQueryPlan::getState() const { return currentState; }

void DecomposedQueryPlan::setState(QueryState newState) { currentState = newState; }

DecomposedQueryPlanVersion DecomposedQueryPlan::getVersion() const { return currentVersion; }

void DecomposedQueryPlan::setVersion(DecomposedQueryPlanVersion newVersion) { currentVersion = newVersion; }

bool DecomposedQueryPlan::hasOperatorWithId(OperatorId operatorId) {
    NES_DEBUG("Checking if the operator exists in the query plan or not");
    if (getOperatorWithId(operatorId)) {
        return true;
    }
    NES_DEBUG("QueryPlan: Unable to find operator with matching Id");
    return false;
}

OperatorNodePtr DecomposedQueryPlan::getOperatorWithId(OperatorId operatorId) {
    NES_DEBUG("Checking if the operator with id {} exists in the query plan or not", operatorId);
    for (auto rootOperator : rootOperators) {

        if (rootOperator->getId() == operatorId) {
            NES_DEBUG("Found operator {} in the query plan", operatorId);
            return rootOperator;
        }

        //Look up in the child operators
        auto matchedOperator = rootOperator->getChildWithOperatorId(operatorId);
        if (matchedOperator) {
            return matchedOperator->as<LogicalOperatorNode>();
        }
    }
    NES_DEBUG("Unable to find operator with matching Id");
    return nullptr;
}

bool DecomposedQueryPlan::replaceRootOperator(const OperatorNodePtr& oldRoot, const OperatorNodePtr& newRoot) {
    for (auto& rootOperator : rootOperators) {
        // compares the pointers and checks if we found the correct operator.
        if (rootOperator == oldRoot) {
            rootOperator = newRoot;
            return true;
        }
    }
    return false;
}

void DecomposedQueryPlan::appendOperatorAsNewRoot(const OperatorNodePtr& operatorNode) {
    NES_DEBUG("QueryPlan: Appending operator {} as new root of the plan.", operatorNode->toString());
    for (const auto& rootOperator : rootOperators) {
        if (!rootOperator->addParent(operatorNode)) {
            NES_THROW_RUNTIME_ERROR("QueryPlan: Unable to add operator " + operatorNode->toString() + " as parent to "
                                    + rootOperator->toString());
        }
    }
    NES_DEBUG("QueryPlan: Clearing current root operators.");
    rootOperators.clear();
    NES_DEBUG("QueryPlan: Pushing input operator node as new root.");
    rootOperators.push_back(operatorNode);
}

std::set<OperatorNodePtr> DecomposedQueryPlan::getAllOperators() {

    // Maintain a list of visited nodes as there are multiple root nodes
    std::set<OperatorNodePtr> visitedOperators;
    NES_DEBUG("QueryPlan: Iterate over all root nodes to find the operator.");
    for (const auto& rootOperator : rootOperators) {
        auto bfsIterator = BreadthFirstNodeIterator(rootOperator);
        for (auto itr = bfsIterator.begin(); itr != BreadthFirstNodeIterator::end(); ++itr) {
            auto visitingOp = (*itr)->as<OperatorNode>();
            if (visitedOperators.contains(visitingOp)) {
                // skip rest of the steps as the node found in already visited node list
                continue;
            }
            NES_DEBUG("QueryPlan: Inserting operator in collection of already visited node.");
            visitedOperators.insert(visitingOp);
        }
    }
    return visitedOperators;
}

DecomposedQueryPlanPtr DecomposedQueryPlan::copy() {
    NES_INFO("QueryPlan: make copy of this query plan");
    // 1. We start by copying the root operators of this query plan to the queue of operators to be processed
    std::map<uint64_t, OperatorNodePtr> operatorIdToOperatorMap;
    std::deque<NodePtr> operatorsToProcess{rootOperators.begin(), rootOperators.end()};
    while (!operatorsToProcess.empty()) {
        auto operatorNode = operatorsToProcess.front()->as<OperatorNode>();
        operatorsToProcess.pop_front();
        uint64_t operatorId = operatorNode->getId();
        // 2. We add each non existing operator to a map and skip adding the operator that already exists in the map.
        // 3. We use the already existing operator whenever available other wise we create a copy of the operator and add it to the map.
        if (operatorIdToOperatorMap[operatorId]) {
            NES_TRACE("QueryPlan: Operator was processed previously");
            operatorNode = operatorIdToOperatorMap[operatorId];
        } else {
            NES_TRACE("QueryPlan: Adding the operator into map");
            operatorIdToOperatorMap[operatorId] = operatorNode->copy();
        }

        // 4. We then check the parent operators of the current operator by looking into the map and add them as the parent of the current operator.
        for (const auto& parentNode : operatorNode->getParents()) {
            auto parentOperator = parentNode->as<OperatorNode>();
            uint64_t parentOperatorId = parentOperator->getId();
            if (operatorIdToOperatorMap.contains(parentOperatorId)) {
                NES_TRACE("QueryPlan: Found the parent operator. Adding as parent to the current operator.");
                parentOperator = operatorIdToOperatorMap[parentOperatorId];
                auto copyOfOperatorNode = operatorIdToOperatorMap[operatorNode->getId()];
                copyOfOperatorNode->addParent(parentOperator);
            } else {
                NES_ERROR("QueryPlan: unable to find the parent operator. This should not have occurred!");
                return nullptr;
            }
        }

        NES_TRACE("QueryPlan: add the child global query nodes for further processing.");
        // 5. We push the children operators to the queue of operators to be processed.
        for (const auto& childrenOperator : operatorNode->getChildren()) {
            operatorsToProcess.push_back(childrenOperator);
        }
    }

    std::vector<OperatorNodePtr> duplicateRootOperators;
    for (const auto& rootOperator : rootOperators) {
        NES_TRACE("QueryPlan: Finding the operator with same id in the map.");
        duplicateRootOperators.push_back(operatorIdToOperatorMap[rootOperator->getId()]);
    }
    operatorIdToOperatorMap.clear();

    // Create the duplicated decomposed query plan
    auto newQueryPlan = DecomposedQueryPlan::create(decomposedQueryPlanId, sharedQueryId);
    for (const auto& rootOperator : duplicateRootOperators) {
        newQueryPlan->addRootOperator(rootOperator->as<LogicalOperatorNode>());
    }
    newQueryPlan->setState(currentState);
    newQueryPlan->setVersion(currentVersion);
    return newQueryPlan;
}

std::string DecomposedQueryPlan::toString() const {
    std::stringstream ss;
    auto dumpHandler = QueryConsoleDumpHandler::create(ss);
    for (const auto& rootOperator : rootOperators) {
        dumpHandler->dump(rootOperator);
    }
    return ss.str();
}

}// namespace NES

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
#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperator.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Util/CompilerConstants.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/QueryConsoleDumpHandler.hpp>
#include <algorithm>
#include <queue>
#include <Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinOperator.hpp>

namespace NES {

DecomposedQueryPlanPtr
DecomposedQueryPlan::create(DecomposedQueryId decomposedQueryId, SharedQueryId sharedQueryId, WorkerId workerId) {
    return std::make_shared<DecomposedQueryPlan>(decomposedQueryId, sharedQueryId, workerId);
}

DecomposedQueryPlanPtr DecomposedQueryPlan::create(DecomposedQueryId decomposedQueryId,
                                                   SharedQueryId sharedQueryId,
                                                   WorkerId workerId,
                                                   std::vector<OperatorPtr> rootOperators) {
    return std::make_shared<DecomposedQueryPlan>(decomposedQueryId, sharedQueryId, workerId, rootOperators);
}

DecomposedQueryPlan::DecomposedQueryPlan(DecomposedQueryId decomposedQueryId, SharedQueryId sharedQueryId, WorkerId workerId)
    : sharedQueryId(sharedQueryId), decomposedQueryId(decomposedQueryId),
      decomposedQueryPlanVersion(INVALID_DECOMPOSED_QUERY_PLAN_VERSION), workerId(workerId) {}

DecomposedQueryPlan::DecomposedQueryPlan(DecomposedQueryId decomposedQueryId,
                                         SharedQueryId sharedQueryId,
                                         WorkerId workerId,
                                         std::vector<OperatorPtr> rootOperators)
    : sharedQueryId(sharedQueryId), decomposedQueryId(decomposedQueryId),
      decomposedQueryPlanVersion(INVALID_DECOMPOSED_QUERY_PLAN_VERSION), workerId(workerId),
      rootOperators(std::move(rootOperators)) {}

void DecomposedQueryPlan::addRootOperator(OperatorPtr newRootOperator) { rootOperators.emplace_back(newRootOperator); }

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

std::vector<OperatorPtr> DecomposedQueryPlan::getRootOperators() const { return rootOperators; }

std::vector<OperatorPtr> DecomposedQueryPlan::getLeafOperators() const {
    // Find all the leaf nodes in the query plan
    NES_DEBUG("QueryPlan: Get all leaf nodes in the query plan.");
    std::vector<OperatorPtr> leafOperators;
    // Maintain a list of visited nodes as there are multiple root nodes
    std::set<OperatorId> visitedOpIds;
    NES_DEBUG("QueryPlan: Iterate over all root nodes to find the operator.");
    for (const auto& rootOperator : rootOperators) {
        auto bfsIterator = BreadthFirstNodeIterator(rootOperator);
        for (auto itr = bfsIterator.begin(); itr != BreadthFirstNodeIterator::end(); ++itr) {
            auto visitingOp = (*itr)->as<Operator>();
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

DecomposedQueryId DecomposedQueryPlan::getDecomposedQueryId() const { return decomposedQueryId; }

SharedQueryId DecomposedQueryPlan::getSharedQueryId() const { return sharedQueryId; }

void DecomposedQueryPlan::setDecomposedQueryPlanId(DecomposedQueryId newDecomposedQueryPlanId) {
    decomposedQueryId = newDecomposedQueryPlanId;
}

std::vector<SourceLogicalOperatorPtr> DecomposedQueryPlan::getSourceOperators() const {
    NES_DEBUG("Get all source operators by traversing all the root nodes.");
    std::set<SourceLogicalOperatorPtr> sourceOperatorsSet;
    for (const auto& rootOperator : rootOperators) {
        auto sourceOperators = rootOperator->getNodesByType<SourceLogicalOperator>();
        NES_DEBUG("Insert all source operators to the collection");
        sourceOperatorsSet.insert(sourceOperators.begin(), sourceOperators.end());
    }
    NES_DEBUG("Found {} source operators.", sourceOperatorsSet.size());
    std::vector<SourceLogicalOperatorPtr> sourceOperators{sourceOperatorsSet.begin(), sourceOperatorsSet.end()};
    return sourceOperators;
}

std::vector<SinkLogicalOperatorPtr> DecomposedQueryPlan::getSinkOperators() const {
    NES_DEBUG("Get all sink operators by traversing all the root nodes.");
    std::vector<SinkLogicalOperatorPtr> sinkOperators;
    for (const auto& rootOperator : rootOperators) {
        auto sinkOperator = rootOperator->as<SinkLogicalOperator>();
        sinkOperators.emplace_back(sinkOperator);
    }
    NES_DEBUG("Found {} sink operators.", sinkOperators.size());
    return sinkOperators;
}

QueryState DecomposedQueryPlan::getState() const { return currentState; }

void DecomposedQueryPlan::refreshOperatorIds() {
    auto shouldSetFileName = false;

    for (const auto& logicalOperator : getAllOperators()) {
        logicalOperator->addProperty(QueryCompilation::LOGICAL_OPERATOR_ID_KEY, logicalOperator->getId());
        logicalOperator->setId(getNextOperatorId());
        if (logicalOperator->as_if<SinkLogicalOperator>() && logicalOperator->as<SinkLogicalOperator>()->getSinkDescriptor()->as_if<FileSinkDescriptor>()) {
            shouldSetFileName = true;
        }
    }

    for (const auto& logicalOperator : getAllOperators()) {
        if (shouldSetFileName) {
            if (logicalOperator->as_if<LogicalJoinOperator>()) {
                std::string file = "/local-ssd/ankit/sr630-wn-a-08-nes-fer-peer/final_file_completed.bin";
//                std::string file = "final_file_completed.bin";
                logicalOperator->addProperty("MIGRATION_FILE", file);
            }
        }
    }
}

void DecomposedQueryPlan::setState(QueryState newState) { currentState = newState; }

DecomposedQueryPlanVersion DecomposedQueryPlan::getVersion() const { return decomposedQueryPlanVersion; }

void DecomposedQueryPlan::setVersion(DecomposedQueryPlanVersion newVersion) { decomposedQueryPlanVersion = newVersion; }

bool DecomposedQueryPlan::hasOperatorWithId(OperatorId operatorId) const {
    NES_DEBUG("Checking if the operator exists in the query plan or not");
    if (getOperatorWithOperatorId(operatorId)) {
        return true;
    }
    NES_DEBUG("QueryPlan: Unable to find operator with matching Id");
    return false;
}

OperatorPtr DecomposedQueryPlan::getOperatorWithOperatorId(OperatorId operatorId) const {
    NES_DEBUG("Checking if the operator with id {} exists in the query plan or not", operatorId);
    for (auto rootOperator : rootOperators) {

        if (rootOperator->getId() == operatorId) {
            NES_DEBUG("Found operator {} in the query plan", operatorId);
            return rootOperator;
        }

        //Look up in the child operators
        auto matchedOperator = rootOperator->getChildWithOperatorId(operatorId);
        if (matchedOperator) {
            return matchedOperator->as<LogicalOperator>();
        }
    }
    NES_DEBUG("Unable to find operator with matching Id");
    return nullptr;
}

bool DecomposedQueryPlan::replaceRootOperator(const OperatorPtr& oldRoot, const OperatorPtr& newRoot) {
    for (auto& rootOperator : rootOperators) {
        // compares the pointers and checks if we found the correct operator.
        if (rootOperator == oldRoot) {
            rootOperator = newRoot;
            return true;
        }
    }
    return false;
}

void DecomposedQueryPlan::appendOperatorAsNewRoot(const OperatorPtr& operatorNode) {
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

std::unordered_set<OperatorPtr> DecomposedQueryPlan::getAllOperators() const {
    // Maintain a list of visited nodes as there are multiple root nodes
    std::unordered_set<OperatorPtr> visitedOperators;
    NES_DEBUG("QueryPlan: Iterate over all root nodes to find the operator.");
    for (const auto& rootOperator : rootOperators) {
        auto bfsIterator = BreadthFirstNodeIterator(rootOperator);
        for (auto itr = bfsIterator.begin(); itr != BreadthFirstNodeIterator::end(); ++itr) {
            auto visitingOp = (*itr)->as<Operator>();
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
    NES_INFO("DecomposedQueryPlan: make copy.");
    // 1. We start by copying the root operators of this query plan to the queue of operators to be processed
    std::map<OperatorId, OperatorPtr> operatorIdToOperatorMap;
    std::deque<NodePtr> operatorsToProcess{rootOperators.begin(), rootOperators.end()};
    while (!operatorsToProcess.empty()) {
        auto operatorNode = operatorsToProcess.front()->as<Operator>();
        operatorsToProcess.pop_front();
        OperatorId operatorId = operatorNode->getId();
        // 2. We add each non existing operator to a map and skip adding the operator that already exists in the map.
        // 3. We use the already existing operator whenever available other wise we create a copy of the operator and add it to the map.
        if (operatorIdToOperatorMap[operatorId]) {
            NES_TRACE("DecomposedQueryPlan: Operator was processed previously");
            operatorNode = operatorIdToOperatorMap[operatorId];
        } else {
            NES_TRACE("DecomposedQueryPlan: Adding the operator into map");
            operatorIdToOperatorMap[operatorId] = operatorNode->copy();
        }

        // 4. We then check the parent operators of the current operator by looking into the map and add them as the parent of the current operator.
        for (const auto& parentNode : operatorNode->getParents()) {
            auto parentOperator = parentNode->as<Operator>();
            OperatorId parentOperatorId = parentOperator->getId();
            if (operatorIdToOperatorMap.contains(parentOperatorId)) {
                NES_TRACE("DecomposedQueryPlan: Found the parent operator. Adding as parent to the current operator.");
                parentOperator = operatorIdToOperatorMap[parentOperatorId];
                auto copyOfOperator = operatorIdToOperatorMap[operatorNode->getId()];
                copyOfOperator->addParent(parentOperator);
            } else {
                NES_ASSERT(false, "DecomposedQueryPlan: Copying the plan failed because parent operator not found.");
            }
        }

        NES_TRACE("DecomposedQueryPlan: add the child global query nodes for further processing.");
        // 5. We push the children operators to the queue of operators to be processed.
        for (const auto& childrenOperator : operatorNode->getChildren()) {

            //Check if all parents were processed
            auto parentOperators = childrenOperator->getParents();
            bool processedAllParent = true;
            for (const auto& parentOperator : parentOperators) {
                if (!operatorIdToOperatorMap.contains(parentOperator->as<Operator>()->getId())) {
                    processedAllParent = false;
                    break;
                }
            }

            // Add child only if all parents were processed
            if (processedAllParent) {
                operatorsToProcess.push_back(childrenOperator);
            }
        }
    }

    std::vector<OperatorPtr> duplicateRootOperators;
    for (const auto& rootOperator : rootOperators) {
        NES_TRACE("DecomposedQueryPlan: Finding the operator with same id in the map.");
        duplicateRootOperators.push_back(operatorIdToOperatorMap[rootOperator->getId()]);
    }
    operatorIdToOperatorMap.clear();

    // Create the duplicated decomposed query plan
    auto copiedDecomposedQueryPlan =
        DecomposedQueryPlan::create(decomposedQueryId, sharedQueryId, workerId, duplicateRootOperators);
    copiedDecomposedQueryPlan->setState(currentState);
    copiedDecomposedQueryPlan->setVersion(decomposedQueryPlanVersion);
    return copiedDecomposedQueryPlan;
}

std::string DecomposedQueryPlan::toString() const {
    std::stringstream ss;
    auto dumpHandler = QueryConsoleDumpHandler::create(ss);
    for (const auto& rootOperator : rootOperators) {
        dumpHandler->dump(rootOperator);
    }
    return ss.str();
}

WorkerId DecomposedQueryPlan::getWorkerId() const { return workerId; }

}// namespace NES

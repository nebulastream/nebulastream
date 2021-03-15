/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Optimizer/QueryMerger/Signature/QuerySignature.hpp>
#include <Plans/Global/Query/GlobalQueryNode.hpp>
#include <Plans/Global/Query/SharedQueryMetaData.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/PlanIdGenerator.hpp>
#include <algorithm>
#include <utility>

namespace NES {

SharedQueryMetaData::SharedQueryMetaData(QueryId queryId, QueryPlanPtr queryPlan)
    : sharedQueryId(PlanIdGenerator::getNextSharedQueryId()), deployed(false), newMetaData(true) {
    NES_DEBUG("SharedQueryMetaData()");
    //Create a new query plan
    this->queryPlan = QueryPlan::create();
    auto rootOperators = queryPlan->getRootOperators();
    for (auto rootOperator : rootOperators) {
        this->queryPlan->addRootOperator(rootOperator);
    }
    this->queryPlan->setQueryId(sharedQueryId);
    queryIdToSinkOperatorMap[queryId] = rootOperators;
    sinkOperators = rootOperators;
    queryIds.push_back(queryId);
}

SharedQueryMetaDataPtr SharedQueryMetaData::create(QueryId queryId, QueryPlanPtr queryPlan) {
    return std::make_shared<SharedQueryMetaData>(SharedQueryMetaData(queryId, queryPlan));
}

bool SharedQueryMetaData::removeQueryId(QueryId queryId) {
    NES_DEBUG("SharedQueryMetaData: Remove the Query Id " << queryId
                                                          << " and associated Global Query Nodes with sink operators.");
    if (queryIdToSinkOperatorMap.find(queryId) == queryIdToSinkOperatorMap.end()) {
        NES_ERROR("SharedQueryMetaData: query id " << queryId << " is not present in metadata information.");
        return false;
    }

    NES_TRACE("SharedQueryMetaData: Remove the Global Query Nodes with sink operators for query " << queryId);
    std::vector<OperatorNodePtr> sinkOperatorsToRemove = queryIdToSinkOperatorMap[queryId];

    // Iterate over all sink global query nodes for the input query and remove the corresponding exclusive upstream operator chains
    for (auto sinkOperator : sinkOperatorsToRemove) {

        //Remove sink operator and associated operators from query plan
        if (!queryPlan->removeRootOperatorFromPlan(sinkOperator)) {
            NES_ERROR("");
            return false;
        }
        //Remove the sink operator from the collection of sink operators in the global query metadata
        sinkOperators.erase(std::remove(sinkOperators.begin(), sinkOperators.end(), sinkOperator), sinkOperators.end());
        NES_ERROR("Unable to find query's sink global query node in the set of Sink global query nodes in the metadata");
        return false;
    }
    queryIdToSinkOperatorMap.erase(queryId);
    return true;
}

void SharedQueryMetaData::markAsNotDeployed() {
    NES_TRACE("SharedQueryMetaData: Mark the Global Query Metadata as updated but not deployed");
    this->deployed = false;
}

void SharedQueryMetaData::markAsDeployed() {
    NES_TRACE("SharedQueryMetaData: Mark the Global Query Metadata as deployed.");
    this->deployed = true;
    this->newMetaData = false;
}

bool SharedQueryMetaData::isEmpty() {
    NES_TRACE("SharedQueryMetaData: Check if Global Query Metadata is empty. Found : " << queryIdToSinkOperatorMap.empty());
    return queryIdToSinkOperatorMap.empty();
}

bool SharedQueryMetaData::isDeployed() const {
    NES_TRACE("SharedQueryMetaData: Checking if Global Query Metadata was already deployed. Found : " << deployed);
    return deployed;
}

bool SharedQueryMetaData::isNew() const {
    NES_TRACE("SharedQueryMetaData: Checking if Global Query Metadata was newly constructed. Found : " << newMetaData);
    return newMetaData;
}

void SharedQueryMetaData::setAsOld() {
    NES_TRACE("SharedQueryMetaData: Marking Global Query Metadata as old post deployment");
    this->newMetaData = false;
}

std::vector<OperatorNodePtr> SharedQueryMetaData::getSinkOperators() {
    NES_TRACE("SharedQueryMetaData: Get all Global Query Nodes with sink operators for the current Metadata");
    return sinkOperators;
}

std::map<QueryId, std::vector<OperatorNodePtr>> SharedQueryMetaData::getQueryIdToSinkOperatorMap() {
    return queryIdToSinkOperatorMap;
}

SharedQueryId SharedQueryMetaData::getSharedQueryId() const { return sharedQueryId; }

void SharedQueryMetaData::clear() {
    NES_DEBUG("SharedQueryMetaData: clearing all metadata information.");
    queryIdToSinkOperatorMap.clear();
    sinkOperators.clear();
    markAsNotDeployed();
}

void SharedQueryMetaData::removeExclusiveChildren(GlobalQueryNodePtr globalQueryNode) {

    for (auto& child : globalQueryNode->getChildren()) {
        if (!child->getParents().empty() && child->getParents().size() == 1) {
            removeExclusiveChildren(child->as<GlobalQueryNode>());
        }
    }
    globalQueryNode->removeChildren();
}

bool SharedQueryMetaData::addSharedQueryMetaData(SharedQueryMetaDataPtr queryMetaData) {

    NES_DEBUG("SharedQueryMetaData: Adding query metadata to this");

    auto newQueryIds = queryMetaData->getQueryIds();
    queryIds.insert(queryIds.end(), newQueryIds.begin(), newQueryIds.end());
    auto newSinkOperators = queryMetaData->getSinkOperators();
    sinkOperators.insert(sinkOperators.end(), newSinkOperators.begin(), newSinkOperators.end());
    auto newQueryIdToSinkOperatorMap = queryMetaData->getQueryIdToSinkOperatorMap();
    queryIdToSinkOperatorMap.insert(newQueryIdToSinkOperatorMap.begin(), newQueryIdToSinkOperatorMap.end());
    //Mark the meta data as updated but not deployed
    markAsNotDeployed();
    return true;
}

std::vector<QueryId> SharedQueryMetaData::getQueryIds() { return queryIds; }

bool SharedQueryMetaData::mergeOperatorInto(OperatorNodePtr operatorToMerge, OperatorNodePtr targetOperator) {

    NES_DEBUG("SharedQueryMetaData: Merging operatorToMerge into target operator");
    auto parentOperatorsToReassign = operatorToMerge->getParents();

    for (auto newParent : parentOperatorsToReassign) {
        bool removedOldChild = newParent->removeChild(operatorToMerge);
        if (!removedOldChild) {
            NES_ERROR("SharedQueryMetaData: Failed to remove old child");
            return false;
        }

        bool addedNewChild = newParent->addChild(targetOperator);
        if (!addedNewChild) {
            NES_ERROR("");
            return false;
        }
    }
    return true;
}

QueryPlanPtr SharedQueryMetaData::getQueryPlan() {

    return queryPlan;
    /*NES_DEBUG("SharedQueryMetaData: Prepare the Query Plan based on the Global Query Metadata information");
    std::vector<OperatorNodePtr> rootOperators;
    std::map<uint64_t, OperatorNodePtr> operatorIdToOperatorMap;

    // We process a Global Query node by extracting its operator and preparing a map of operator id to operator.
    // We push the children operators to the queue of operators to be processed.
    // Every time we encounter an operator, we check in the map if the operator with same id already exists.
    // We use the already existing operator whenever available other wise we create a copy of the operator and add it to the map.
    // We then check the parent operators of the current operator by looking into the parent Global Query Nodes of the current
    // Global Query Node and add them as the parent of the current operator.

    std::deque<NodePtr> globalQueryNodesToProcess{sinkGlobalQueryNodes.begin(), sinkGlobalQueryNodes.end()};
    while (!globalQueryNodesToProcess.empty()) {
        auto gqnToProcess = globalQueryNodesToProcess.front()->as<GlobalQueryNode>();
        globalQueryNodesToProcess.pop_front();
        NES_TRACE("SharedQueryMetaData: Deserialize operator " << gqnToProcess->toString());
        OperatorNodePtr operatorNode = gqnToProcess->getOperator()->copy();
        uint64_t operatorId = operatorNode->getId();
        if (operatorIdToOperatorMap[operatorId]) {
            NES_TRACE("SharedQueryMetaData: Operator was already deserialized previously");
            operatorNode = operatorIdToOperatorMap[operatorId];
        } else {
            NES_TRACE("SharedQueryMetaData: Deserializing the operator and inserting into map");
            operatorIdToOperatorMap[operatorId] = operatorNode;
        }

        for (const auto& parentNode : gqnToProcess->getParents()) {
            auto parentGQN = parentNode->as<GlobalQueryNode>();
            if (parentGQN->getId() == 0) {
                continue;
            }
            OperatorNodePtr parentOperator = parentGQN->getOperator();
            uint64_t parentOperatorId = parentOperator->getId();
            if (operatorIdToOperatorMap[parentOperatorId]) {
                NES_TRACE("SharedQueryMetaData: Found the parent operator. Adding as parent to the current operator.");
                parentOperator = operatorIdToOperatorMap[parentOperatorId];
                operatorNode->addParent(parentOperator);
            } else {
                NES_ERROR("SharedQueryMetaData: unable to find the parent operator. This should not have occurred!");
                return nullptr;
            }
        }

        NES_TRACE("SharedQueryMetaData: add the child global query nodes for further processing.");
        for (const auto& childGQN : gqnToProcess->getChildren()) {
            globalQueryNodesToProcess.push_back(childGQN);
        }
    }

    for (const auto& sinkGlobalQueryNode : sinkGlobalQueryNodes) {
        NES_TRACE("SharedQueryMetaData: Finding the operator with same id in the map.");
        auto rootOperator = sinkGlobalQueryNode->getOperator()->copy();
        rootOperator = operatorIdToOperatorMap[rootOperator->getId()];
        NES_TRACE("SharedQueryMetaData: Adding the root operator to the vector of roots for the query plan");
        rootOperators.push_back(rootOperator);
    }
    operatorIdToOperatorMap.clear();
    return QueryPlan::create(sharedQueryId, INVALID_QUERY_ID, rootOperators);*/
}

}// namespace NES

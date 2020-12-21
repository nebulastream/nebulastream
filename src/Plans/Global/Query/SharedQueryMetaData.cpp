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

SharedQueryMetaData::SharedQueryMetaData(QueryId queryId, std::set<GlobalQueryNodePtr> sinkGlobalQueryNodes)
    : sharedQueryId(PlanIdGenerator::getNextSharedQueryId()), sinkGlobalQueryNodes(std::move(sinkGlobalQueryNodes)),
      deployed(false), newMetaData(true) {
    NES_DEBUG("SharedQueryMetaData()");
    queryIdToSinkGQNMap[queryId] = this->sinkGlobalQueryNodes;
}

SharedQueryMetaDataPtr SharedQueryMetaData::create(QueryId queryId, std::set<GlobalQueryNodePtr> sinkGlobalQueryNodes) {
    return std::make_shared<SharedQueryMetaData>(SharedQueryMetaData(queryId, std::move(sinkGlobalQueryNodes)));
}

bool SharedQueryMetaData::addSinkGlobalQueryNodes(QueryId queryId, const std::set<GlobalQueryNodePtr>& globalQueryNodes) {

    NES_DEBUG("SharedQueryMetaData: Add " << globalQueryNodes.size() << "new Global Query Serialization with sink operators for query "
                                          << queryId);
    if (queryIdToSinkGQNMap.find(queryId) != queryIdToSinkGQNMap.end()) {
        NES_ERROR("SharedQueryMetaData: query id " << queryId << " already present in metadata information.");
        return false;
    }

    NES_TRACE("SharedQueryMetaData: Inserting new entries into metadata");
    queryIdToSinkGQNMap[queryId] = globalQueryNodes;
    sinkGlobalQueryNodes.insert(globalQueryNodes.begin(), globalQueryNodes.end());
    //Mark the meta data as updated but not deployed
    markAsNotDeployed();
    return true;
}

bool SharedQueryMetaData::removeQueryId(QueryId queryId) {
    NES_DEBUG("SharedQueryMetaData: Remove the Query Id " << queryId
                                                          << " and associated Global Query Serialization with sink operators.");
    if (queryIdToSinkGQNMap.find(queryId) == queryIdToSinkGQNMap.end()) {
        NES_ERROR("SharedQueryMetaData: query id " << queryId << " is not present in metadata information.");
        return false;
    }

    NES_TRACE("SharedQueryMetaData: Remove the Global Query Serialization with sink operators for query " << queryId);
    std::set<GlobalQueryNodePtr> querySinkGQNs = queryIdToSinkGQNMap[queryId];

    // Iterate over all sink global query nodes for the input query and remove the corresponding exclusive upstream operator chains
    for (auto& querySinkGQN : querySinkGQNs) {
        auto found =
            std::find_if(sinkGlobalQueryNodes.begin(), sinkGlobalQueryNodes.end(), [querySinkGQN](GlobalQueryNodePtr sinkGQN) {
                return querySinkGQN->getId() == sinkGQN->getId();
            });

        if (found != sinkGlobalQueryNodes.end()) {
            //Remove for the sink global query node the corresponding parent and children nodes
            (*found)->removeAllParent();
            removeExclusiveChildren(*found);
            //Remove the sink global query node from the collection of sink global query nodes
            sinkGlobalQueryNodes.erase(found);
        } else {
            NES_ERROR("Unable to find query's sink global query node in the set of Sink global query nodes in the metadata");
            return false;
        }
    }
    queryIdToSinkGQNMap.erase(queryId);
    //Mark the meta data as updated but not deployed
    markAsNotDeployed();
    return true;
}

QueryPlanPtr SharedQueryMetaData::getQueryPlan() {

    NES_DEBUG("SharedQueryMetaData: Prepare the Query Plan based on the Global Query Metadata information");
    std::vector<OperatorNodePtr> rootOperators;
    std::map<uint64_t, OperatorNodePtr> operatorIdToOperatorMap;

    // We process a Global Query node by extracting its operator and preparing a map of operator id to operator.
    // We push the children operators to the queue of operators to be processed.
    // Every time we encounter an operator, we check in the map if the operator with same id already exists.
    // We use the already existing operator whenever available other wise we create a copy of the operator and add it to the map.
    // We then check the parent operators of the current operator by looking into the parent Global Query Serialization of the current
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
    return QueryPlan::create(sharedQueryId, INVALID_QUERY_ID, rootOperators);
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
    NES_TRACE("SharedQueryMetaData: Check if Global Query Metadata is empty. Found : " << queryIdToSinkGQNMap.empty());
    return queryIdToSinkGQNMap.empty();
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

std::set<GlobalQueryNodePtr> SharedQueryMetaData::getSinkGlobalQueryNodes() {
    NES_TRACE("SharedQueryMetaData: Get all Global Query Serialization with sink operators for the current Metadata");
    return sinkGlobalQueryNodes;
}

std::map<QueryId, std::set<GlobalQueryNodePtr>> SharedQueryMetaData::getQueryIdToSinkGQNMap() { return queryIdToSinkGQNMap; }

SharedQueryId SharedQueryMetaData::getSharedQueryId() const { return sharedQueryId; }

void SharedQueryMetaData::clear() {
    NES_DEBUG("SharedQueryMetaData: clearing all metadata information.");
    queryIdToSinkGQNMap.clear();
    sinkGlobalQueryNodes.clear();
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

    auto queryIdToSinkGqnMap = queryMetaData->getQueryIdToSinkGQNMap();
    for (auto [queryId, sinkGQNs] : queryIdToSinkGqnMap) {
        if (!addSinkGlobalQueryNodes(queryId, sinkGQNs)) {
            NES_WARNING("Failed to insert Sink GQN for query " << queryId << " in Global Query Meta Data.");
            NES_DEBUG("Reverting all inserted Sink GQNs from global query metadata.");
            for (auto [queryId, sinkGQNs] : queryIdToSinkGqnMap) {
                removeQueryId(queryId);
            }
            return false;
        }
    }
    return true;
}

}// namespace NES

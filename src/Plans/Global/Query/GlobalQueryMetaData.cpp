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
#include <Plans/Global/Query/GlobalQueryMetaData.hpp>
#include <Plans/Global/Query/GlobalQueryNode.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/PlanIdGenerator.hpp>
#include <algorithm>
#include <utility>

namespace NES {

GlobalQueryMetaData::GlobalQueryMetaData(QueryId queryId, std::set<GlobalQueryNodePtr> sinkGlobalQueryNodes)
    : globalQueryId(PlanIdGenerator::getNextGlobalQueryId()), sinkGlobalQueryNodes(std::move(sinkGlobalQueryNodes)),
      deployed(false), newMetaData(true) {
    NES_DEBUG("GlobalQueryMetaData()");
    queryIdToSinkGQNMap[queryId] = sinkGlobalQueryNodes;
}

GlobalQueryMetaDataPtr GlobalQueryMetaData::create(QueryId queryId, std::set<GlobalQueryNodePtr> sinkGlobalQueryNodes) {
    return std::make_shared<GlobalQueryMetaData>(GlobalQueryMetaData(queryId, std::move(sinkGlobalQueryNodes)));
}

bool GlobalQueryMetaData::addSinkGlobalQueryNodes(QueryId queryId, const std::set<GlobalQueryNodePtr>& globalQueryNodes) {

    NES_DEBUG("GlobalQueryMetaData: Add " << globalQueryNodes.size() << "new Global Query Nodes with sink operators for query "
                                          << queryId);
    if (queryIdToSinkGQNMap.find(queryId) != queryIdToSinkGQNMap.end()) {
        NES_ERROR("GlobalQueryMetaData: query id " << queryId << " already present in metadata information.");
        return false;
    }

    NES_TRACE("GlobalQueryMetaData: Inserting new entries into metadata");
    queryIdToSinkGQNMap[queryId] = globalQueryNodes;
    sinkGlobalQueryNodes.insert(globalQueryNodes.begin(), globalQueryNodes.end());
    //Mark the meta data as updated but not deployed
    markAsNotDeployed();
    return true;
}

bool GlobalQueryMetaData::removeQueryId(QueryId queryId) {
    NES_DEBUG("GlobalQueryMetaData: Remove the Query Id " << queryId
                                                          << " and associated Global Query Nodes with sink operators.");
    if (queryIdToSinkGQNMap.find(queryId) == queryIdToSinkGQNMap.end()) {
        NES_ERROR("GlobalQueryMetaData: query id " << queryId << " is not present in metadata information.");
        return false;
    }

    NES_TRACE("GlobalQueryMetaData: Remove the Global Query Nodes with sink operators for query " << queryId);
    std::set<GlobalQueryNodePtr> querySinkGQNs = queryIdToSinkGQNMap[queryId];
    auto itr = sinkGlobalQueryNodes.begin();
    for (auto& querySinkGQN : querySinkGQNs) {
        auto found =
            std::find_if(sinkGlobalQueryNodes.begin(), sinkGlobalQueryNodes.end(), [querySinkGQN](GlobalQueryNodePtr sinkGQN) {
                return querySinkGQN->getId() == sinkGQN->getId();
            });

        if (found != sinkGlobalQueryNodes.end()) {
            sinkGlobalQueryNodes.erase(found);
            (*found)->removeAllParent();
            removeExclusiveChildren(*found);
        } else {
            NES_ERROR("Unable to find query's sink global query node in the set of Sink global query nodes in the metadata");
            return false;
        }
    }
    //Mark the meta data as updated but not deployed
    markAsNotDeployed();
    return true;
}

QueryPlanPtr GlobalQueryMetaData::getQueryPlan() {

    NES_DEBUG("GlobalQueryMetaData: Prepare the Query Plan based on the Global Query Metadata information");
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
        NES_TRACE("GlobalQueryMetaData: Deserialize operator " << gqnToProcess->toString());
        OperatorNodePtr operatorNode = gqnToProcess->getOperator()->copy();
        uint64_t operatorId = operatorNode->getId();
        if (operatorIdToOperatorMap[operatorId]) {
            NES_TRACE("GlobalQueryMetaData: Operator was already deserialized previously");
            operatorNode = operatorIdToOperatorMap[operatorId];
        } else {
            NES_TRACE("GlobalQueryMetaData: Deserializing the operator and inserting into map");
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
                NES_TRACE("GlobalQueryMetaData: Found the parent operator. Adding as parent to the current operator.");
                parentOperator = operatorIdToOperatorMap[parentOperatorId];
                operatorNode->addParent(parentOperator);
            } else {
                NES_ERROR("GlobalQueryMetaData: unable to find the parent operator. This should not have occurred!");
                return nullptr;
            }
        }

        NES_TRACE("GlobalQueryMetaData: add the child global query nodes for further processing.");
        for (const auto& childGQN : gqnToProcess->getChildren()) {
            globalQueryNodesToProcess.push_back(childGQN);
        }
    }

    for (const auto& sinkGlobalQueryNode : sinkGlobalQueryNodes) {
        NES_TRACE("GlobalQueryMetaData: Finding the operator with same id in the map.");
        auto rootOperator = sinkGlobalQueryNode->getOperator()->copy();
        rootOperator = operatorIdToOperatorMap[rootOperator->getId()];
        NES_TRACE("GlobalQueryMetaData: Adding the root operator to the vector of roots for the query plan");
        rootOperators.push_back(rootOperator);
    }
    operatorIdToOperatorMap.clear();
    return QueryPlan::create(globalQueryId, INVALID_QUERY_ID, rootOperators);
}

void GlobalQueryMetaData::markAsNotDeployed() {
    NES_TRACE("GlobalQueryMetaData: Mark the Global Query Metadata as updated but not deployed");
    this->deployed = false;
}

void GlobalQueryMetaData::markAsDeployed() {
    NES_TRACE("GlobalQueryMetaData: Mark the Global Query Metadata as deployed.");
    this->deployed = true;
    this->newMetaData = false;
}

bool GlobalQueryMetaData::isEmpty() {
    NES_TRACE("GlobalQueryMetaData: Check if Global Query Metadata is empty. Found : " << queryIdToSinkGQNMap.empty());
    return queryIdToSinkGQNMap.empty();
}

bool GlobalQueryMetaData::isDeployed() const {
    NES_TRACE("GlobalQueryMetaData: Checking if Global Query Metadata was already deployed. Found : " << deployed);
    return deployed;
}

bool GlobalQueryMetaData::isNew() const {
    NES_TRACE("GlobalQueryMetaData: Checking if Global Query Metadata was newly constructed. Found : " << newMetaData);
    return newMetaData;
}

void GlobalQueryMetaData::setAsOld() {
    NES_TRACE("GlobalQueryMetaData: Marking Global Query Metadata as old post deployment");
    this->newMetaData = false;
}

std::set<GlobalQueryNodePtr> GlobalQueryMetaData::getSinkGlobalQueryNodes() {
    NES_TRACE("GlobalQueryMetaData: Get all Global Query Nodes with sink operators for the current Metadata");
    return sinkGlobalQueryNodes;
}

std::map<QueryId, std::set<GlobalQueryNodePtr>> GlobalQueryMetaData::getQueryIdToSinkGQNMap() { return queryIdToSinkGQNMap; }

GlobalQueryId GlobalQueryMetaData::getGlobalQueryId() const { return globalQueryId; }

void GlobalQueryMetaData::clear() {
    NES_DEBUG("GlobalQueryMetaData: clearing all metadata information.");
    queryIdToSinkGQNMap.clear();
    sinkGlobalQueryNodes.clear();
    markAsNotDeployed();
}

void GlobalQueryMetaData::removeExclusiveChildren(GlobalQueryNodePtr globalQueryNode) {

    for(auto& child : globalQueryNode->getChildren()){
        if(child->getParents().empty() || child->getParents().size() == 1){
            child->removeAllParent();
            removeExclusiveChildren(child->as<GlobalQueryNode>());
        }
    }
    globalQueryNode->removeChildren();
}

bool GlobalQueryMetaData::addGlobalQueryMetaData(GlobalQueryMetaDataPtr queryMetaData) {

    auto queryIdToSinkGqnMap = queryMetaData->getQueryIdToSinkGQNMap();
    for(auto[queryId, sinkGQNs] : queryIdToSinkGqnMap){
        if(!addSinkGlobalQueryNodes(queryId, sinkGQNs)){
            NES_WARNING("Failed to insert Sink GQN for query "<< queryId << " in Global Query Meta Data.");
            NES_DEBUG("Reverting all inserted Sink GQNs from global query metadata.");
            for(auto[queryId, sinkGQNs] : queryIdToSinkGqnMap){
                removeQueryId(queryId);
            }
            return false;
        }
    }
    return true;
}

}// namespace NES

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
#include <Optimizer/QueryMerger/Signature/QueryPlanSignature.hpp>
#include <Plans/Global/Query/GlobalQueryMetaData.hpp>
#include <Plans/Global/Query/GlobalQueryNode.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/PlanIdGenerator.hpp>
#include <algorithm>
#include <utility>

namespace NES {

GlobalQueryMetaData::GlobalQueryMetaData(std::set<QueryId> queryIds, std::set<GlobalQueryNodePtr> sinkGlobalQueryNodes)
    : globalQueryId(PlanIdGenerator::getNextGlobalQueryId()), queryIds(std::move(queryIds)),
      sinkGlobalQueryNodes(std::move(sinkGlobalQueryNodes)), deployed(false), newMetaData(true) {}

GlobalQueryMetaDataPtr GlobalQueryMetaData::create(std::set<QueryId> queryIds,
                                                   std::set<GlobalQueryNodePtr> sinkGlobalQueryNodes) {
    return std::make_shared<GlobalQueryMetaData>(GlobalQueryMetaData(std::move(queryIds), std::move(sinkGlobalQueryNodes)));
}

void GlobalQueryMetaData::addNewSinkGlobalQueryNodes(const std::set<GlobalQueryNodePtr>& globalQueryNodes) {

    NES_DEBUG("GlobalQueryMetaData: Add" << globalQueryNodes.size() << "new Global Query Nodes with sink operators");
    for (auto& sinkGQN : globalQueryNodes) {
        //As GQNs with sink operators do not contain more than 1 query get the query id at 0th index.
        queryIds.insert(sinkGQN->getQueryIds()[0]);
        this->sinkGlobalQueryNodes.insert(sinkGQN);
    }
    //Mark the meta data as updated but not deployed
    markAsNotDeployed();
}

bool GlobalQueryMetaData::removeQueryId(QueryId queryId) {
    NES_DEBUG("GlobalQueryMetaData: Remove the Query Id " << queryId
                                                          << " and associated Global Query Nodes with sink operators.");
    auto found = std::find(queryIds.begin(), queryIds.end(), queryId);
    if (found == queryIds.end()) {
        NES_WARNING("GlobalQueryMetaData: Unable to find query Id " << queryId << " in Global Query Metadata " << globalQueryId);
        return false;
    }

    NES_TRACE("GlobalQueryMetaData: Remove the Global Query Nodes with sink operators for query " << queryId);
    auto itr = sinkGlobalQueryNodes.begin();
    while (itr != sinkGlobalQueryNodes.end()) {
        if ((*itr)->hasQuery(queryId)) {
            itr = sinkGlobalQueryNodes.erase(itr);
            (*itr)->removeAllParent();
            (*itr)->removeChildren();
            continue;
        }
        ++itr;
    }
    queryIds.erase(found);
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
        OperatorNodePtr operatorNode = gqnToProcess->getOperators()[0]->copy();
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
            OperatorNodePtr parentOperator = parentGQN->getOperators()[0];
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
        auto rootOperator = sinkGlobalQueryNode->getOperators()[0]->copy();
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
    NES_TRACE("GlobalQueryMetaData: Check if Global Query Metadata is empty. Found : " << queryIds.empty());
    return queryIds.empty();
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

std::set<QueryId> GlobalQueryMetaData::getQueryIds() { return queryIds; }

GlobalQueryId GlobalQueryMetaData::getGlobalQueryId() const { return globalQueryId; }

void GlobalQueryMetaData::clear() {
    NES_DEBUG("GlobalQueryMetaData: clearing all metadata information.");
    queryIds.clear();
    sinkGlobalQueryNodes.clear();
    markAsNotDeployed();
}

bool GlobalQueryMetaData::mergeGlobalQueryMetaData(GlobalQueryMetaDataPtr targetGQM) {

    std::set<GlobalQueryNodePtr> targetSinkGQNs = targetGQM->getSinkGlobalQueryNodes();

    for (auto& targetSinkGQN : targetSinkGQNs) {
        auto targetSignature = targetSinkGQN->getOperators()[0]->as<LogicalOperatorNode>()->getSignature();
        bool foundHost = false;
        for (auto& hostSinkGQN : sinkGlobalQueryNodes) {
            auto hostSignature = targetSinkGQN->getOperators()[0]->as<LogicalOperatorNode>()->getSignature();
            if (hostSignature->isEqual(targetSignature)) {
                targetSinkGQN->removeChildren();
                for (auto& child : hostSinkGQN->getChildren()) {
                    targetSinkGQN->addChild(child);
                }
                foundHost = true;
                break;
            }
        }
        if (!foundHost) {
            return false;
        }
    }

    std::set<QueryId> targetQueryIds = targetGQM->getQueryIds();
    queryIds.insert(targetQueryIds.begin(), targetQueryIds.end());
    targetGQM->clear();
    return true;
}

}// namespace NES

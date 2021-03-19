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

SharedQueryMetaData::SharedQueryMetaData(QueryPlanPtr queryPlan)
    : sharedQueryId(PlanIdGenerator::getNextSharedQueryId()), deployed(false), newMetaData(true) {
    NES_DEBUG("SharedQueryMetaData()");
    auto queryId = queryPlan->getQueryId();
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

SharedQueryMetaDataPtr SharedQueryMetaData::create(QueryPlanPtr queryPlan) {
    return std::make_shared<SharedQueryMetaData>(SharedQueryMetaData(queryPlan));
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
            NES_ERROR("SharedQueryMetaData: ");
            return false;
        }
        //Remove the sink operator from the collection of sink operators in the global query metadata
        sinkOperators.erase(std::remove(sinkOperators.begin(), sinkOperators.end(), sinkOperator), sinkOperators.end());
    }
    queryIdToSinkOperatorMap.erase(queryId);
    //Mark the meta data as updated but not deployed
    markAsNotDeployed();
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
    queryIds.clear();
    markAsNotDeployed();
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

QueryPlanPtr SharedQueryMetaData::getQueryPlan() { return queryPlan; }
}// namespace NES

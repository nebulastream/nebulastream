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
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlanChangeLog.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/PlanIdGenerator.hpp>
#include <algorithm>
#include <utility>

namespace NES {

SharedQueryPlan::SharedQueryPlan(const QueryPlanPtr& queryPlan)
    : sharedQueryId(PlanIdGenerator::getNextSharedQueryId()), deployed(false), newMetaData(true) {
    NES_DEBUG("SharedQueryPlan()");
    auto queryId = queryPlan->getQueryId();
    //Create a new query plan
    this->queryPlan = QueryPlan::create();
    auto rootOperators = queryPlan->getRootOperators();
    for (const auto& rootOperator : rootOperators) {
        this->queryPlan->addRootOperator(rootOperator);
    }
    this->queryPlan->setQueryId(sharedQueryId);
    queryIdToSinkOperatorMap[queryId] = rootOperators;
    changeLog = SharedQueryPlanChangeLog::create();
    sinkOperators = rootOperators;
    queryIds.push_back(queryId);
}

SharedQueryPlanPtr SharedQueryPlan::create(QueryPlanPtr queryPlan) {
    return std::make_shared<SharedQueryPlan>(SharedQueryPlan(std::move(queryPlan)));
}

bool SharedQueryPlan::removeQuery(QueryId queryId) {
    NES_DEBUG("SharedQueryPlan: Remove the Query Id " << queryId << " and associated Global Query Nodes with sink operators.");
    if (queryIdToSinkOperatorMap.find(queryId) == queryIdToSinkOperatorMap.end()) {
        NES_ERROR("SharedQueryPlan: query id " << queryId << " is not present in metadata information.");
        return false;
    }
    NES_TRACE("SharedQueryPlan: Remove the Global Query Nodes with sink operators for query " << queryId);
    std::vector<OperatorNodePtr> sinkOperatorsToRemove = queryIdToSinkOperatorMap[queryId];
    // Iterate over all sink global query nodes for the input query and remove the corresponding exclusive upstream operator chains
    for (const auto& sinkOperator : sinkOperatorsToRemove) {
        //Remove sink operator and associated operators from query plan
        if (!removeOperator(sinkOperator)) {
            NES_ERROR("SharedQueryPlan: unable to remove Root operator from the shared query plan " << sharedQueryId);
            return false;
        }
        queryPlan->removeAsRootOperator(sinkOperator);
        changeLog->registerRemovedSink(sinkOperator->getId());
        //Remove the sink operator from the collection of sink operators in the global query metadata
        sinkOperators.erase(std::remove(sinkOperators.begin(), sinkOperators.end(), sinkOperator), sinkOperators.end());
    }

    queryIdToSinkOperatorMap.erase(queryId);
    //Mark the meta data as updated but not deployed
    markAsNotDeployed();
    return true;
}

void SharedQueryPlan::markAsNotDeployed() {
    NES_TRACE("SharedQueryPlan: Mark the Global Query Metadata as updated but not deployed");
    this->deployed = false;
}

void SharedQueryPlan::markAsDeployed() {
    NES_TRACE("SharedQueryPlan: Mark the Global Query Metadata as deployed.");
    this->deployed = true;
    this->newMetaData = false;
}

bool SharedQueryPlan::isEmpty() {
    NES_TRACE("SharedQueryPlan: Check if Global Query Metadata is empty. Found : " << queryIdToSinkOperatorMap.empty());
    return queryIdToSinkOperatorMap.empty();
}

bool SharedQueryPlan::isDeployed() const {
    NES_TRACE("SharedQueryPlan: Checking if Global Query Metadata was already deployed. Found : " << deployed);
    return deployed;
}

bool SharedQueryPlan::isNew() const {
    NES_TRACE("SharedQueryPlan: Checking if Global Query Metadata was newly constructed. Found : " << newMetaData);
    return newMetaData;
}

void SharedQueryPlan::setAsOld() {
    NES_TRACE("SharedQueryPlan: Marking Global Query Metadata as old post deployment");
    this->newMetaData = false;
}

std::vector<OperatorNodePtr> SharedQueryPlan::getSinkOperators() {
    NES_TRACE("SharedQueryPlan: Get all Global Query Nodes with sink operators for the current Metadata");
    return sinkOperators;
}

std::map<QueryId, std::vector<OperatorNodePtr>> SharedQueryPlan::getQueryIdToSinkOperatorMap() {
    return queryIdToSinkOperatorMap;
}

SharedQueryId SharedQueryPlan::getSharedQueryId() const { return sharedQueryId; }

void SharedQueryPlan::clear() {
    NES_DEBUG("SharedQueryPlan: clearing all metadata information.");
    queryIdToSinkOperatorMap.clear();
    sinkOperators.clear();
    queryIds.clear();
    markAsNotDeployed();
}

std::vector<QueryId> SharedQueryPlan::getQueryIds() { return queryIds; }

QueryPlanPtr SharedQueryPlan::getQueryPlan() { return queryPlan; }

bool SharedQueryPlan::addQueryIdAndSinkOperators(const QueryPlanPtr& queryPlan) {
    auto queryId = queryPlan->getQueryId();
    queryIds.emplace_back(queryId);
    for (const auto& sinkOperator : queryPlan->getRootOperators()) {
        sinkOperators.emplace_back(sinkOperator);
        changeLog->registerNewlyAddedSink(sinkOperator->getId());
        queryIdToSinkOperatorMap[queryId] = queryPlan->getRootOperators();
    }
    //Mark the meta data as updated but not deployed
    markAsNotDeployed();
    return false;
}

void SharedQueryPlan::addAdditionToChangeLog(const OperatorNodePtr& upstreamOperator, const OperatorNodePtr& newOperator) {
    changeLog->addAddition(upstreamOperator, newOperator->getId());
}

bool SharedQueryPlan::removeOperator(const OperatorNodePtr& operatorToRemove) {
    //Iterate over all child operator
    auto children = operatorToRemove->getChildren();
    for (const auto& child : children) {
        auto childOperator = child->as<OperatorNode>();
        //If the child is shared by multiple parents then remove the parent and add it
        // in the change log.
        if (child->getParents().size() > 1) {
            changeLog->addRemoval(childOperator, operatorToRemove->getId());
            childOperator->removeParent(operatorToRemove);
            return true;
        }
        //Remove the parent and call remove operator for children
        childOperator->removeParent(operatorToRemove);
        if (!removeOperator(childOperator)) {
            NES_ERROR("QueryPlan: unable to remove operator " << childOperator->toString() << " from shared query plan "
                                                              << sharedQueryId);
            return false;
        }
    }
    return true;
}

}// namespace NES

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

#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/OperatorNode.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>

namespace NES {

GlobalQueryPlan::GlobalQueryPlan() = default;

GlobalQueryPlanPtr GlobalQueryPlan::create() { return std::make_shared<GlobalQueryPlan>(GlobalQueryPlan()); }

bool GlobalQueryPlan::addQueryPlan(const QueryPlanPtr& queryPlan) {
    QueryId inputQueryPlanId = queryPlan->getQueryId();
    if (inputQueryPlanId == INVALID_QUERY_ID) {
        throw Exception("GlobalQueryPlan: Can not add query plan with invalid id.");
    }

    if (queryIdToSharedQueryIdMap.find(inputQueryPlanId) != queryIdToSharedQueryIdMap.end()) {
        throw Exception("GlobalQueryPlan: Query plan with id " + std::to_string(inputQueryPlanId) + " already present.");
    }

    queryPlansToAdd.emplace_back(queryPlan);
    return true;
}

void GlobalQueryPlan::removeQuery(QueryId queryId) {
    NES_DEBUG("GlobalQueryPlan: Removing query information from the meta data");
    //Check if the query id present in the Global query Plan
    if (queryIdToSharedQueryIdMap.find(queryId) != queryIdToSharedQueryIdMap.end()) {
        //Fetch the shared query plan id and remove the query and associated operators
        SharedQueryId sharedQueryId = queryIdToSharedQueryIdMap[queryId];
        SharedQueryPlanPtr sharedQueryPlan = sharedQueryIdToPlanMap[sharedQueryId];
        if (!sharedQueryPlan->removeQuery(queryId)) {
            throw Exception("GlobalQueryPlan: Unable to remove query with id " + std::to_string(queryId)
                            + " from shared query plan with id " + std::to_string(sharedQueryId));
        }
        //Remove from the queryId to shared query id map
        queryIdToSharedQueryIdMap.erase(queryId);
    } else {
        // Check if the query is in the list of query plans and remove it
        queryPlansToAdd.erase(
            std::find_if(queryPlansToAdd.begin(), queryPlansToAdd.end(), [&queryId](const QueryPlanPtr& queryPlan) {
                return queryPlan->getQueryId() == queryId;
            }));
    }
}

std::vector<SharedQueryPlanPtr> GlobalQueryPlan::getSharedQueryPlansToDeploy() {
    NES_DEBUG("GlobalQueryPlan: Get the Global MetaData to be deployed.");
    std::vector<SharedQueryPlanPtr> sharedQueryMetaDataToDeploy;
    NES_TRACE("GlobalQueryPlan: Iterate over the Map with global query metadata.");
    for (auto& [sharedQueryId, sharedQueryMetaData] : sharedQueryIdToPlanMap) {
        if (sharedQueryMetaData->isDeployed()) {
            NES_TRACE("GlobalQueryPlan: Skipping! found already deployed query meta data.");
            continue;
        }
        sharedQueryMetaDataToDeploy.push_back(sharedQueryMetaData);
    }
    NES_DEBUG("GlobalQueryPlan: Found " << sharedQueryMetaDataToDeploy.size() << "  Shared Query MetaData to be deployed.");
    return sharedQueryMetaDataToDeploy;
}

SharedQueryId GlobalQueryPlan::getSharedQueryId(QueryId queryId) {
    NES_DEBUG("GlobalQueryPlan: Get the Global Query Id for the query " << queryId);
    if (queryIdToSharedQueryIdMap.find(queryId) != queryIdToSharedQueryIdMap.end()) {
        return queryIdToSharedQueryIdMap[queryId];
    }
    NES_WARNING("GlobalQueryPlan: Unable to find Global Query Id for the query " << queryId);
    return INVALID_SHARED_QUERY_ID;
}

bool GlobalQueryPlan::updateSharedQueryPlan(const SharedQueryPlanPtr& sharedQueryPlan) {
    NES_INFO("GlobalQueryPlan: updating the shared query metadata information");
    auto sharedQueryId = sharedQueryPlan->getSharedQueryId();
    sharedQueryIdToPlanMap[sharedQueryId] = sharedQueryPlan;
    NES_TRACE("GlobalQueryPlan: Updating the Query Id to Shared Query Id map");
    for (auto queryId : sharedQueryPlan->getQueryIds()) {
        queryIdToSharedQueryIdMap[queryId] = sharedQueryId;
    }
    return true;
}

void GlobalQueryPlan::removeEmptySharedQueryPlans() {
    NES_INFO("GlobalQueryPlan: remove empty metadata information.");
    //Following associative-container erase idiom
    for (auto itr = sharedQueryIdToPlanMap.begin(); itr != sharedQueryIdToPlanMap.end();) {
        auto sharedQueryMetaData = itr->second;
        if ((sharedQueryMetaData->isDeployed() || sharedQueryMetaData->isNew()) && sharedQueryMetaData->isEmpty()) {
            NES_TRACE("GlobalQueryPlan: Removing! found an empty query meta data.");
            sharedQueryIdToPlanMap.erase(itr++);
            continue;
        }
        itr++;
    }
}

std::vector<SharedQueryPlanPtr> GlobalQueryPlan::getAllSharedQueryPlans() {
    NES_INFO("GlobalQueryPlan: Get all metadata information");
    std::vector<SharedQueryPlanPtr> sharedQueryMetaDataToDeploy;
    NES_TRACE("GlobalQueryPlan: Iterate over the Map of shared query metadata.");
    for (auto& [sharedQueryId, sharedQueryMetaData] : sharedQueryIdToPlanMap) {
        sharedQueryMetaDataToDeploy.emplace_back(sharedQueryMetaData);
    }
    NES_TRACE("GlobalQueryPlan: Found " << sharedQueryMetaDataToDeploy.size() << "  Shared Query MetaData.");
    return sharedQueryMetaDataToDeploy;
}

std::vector<SharedQueryPlanPtr> GlobalQueryPlan::getAllNewSharedQueryPlans() {
    NES_INFO("GlobalQueryPlan: Get all metadata information");
    std::vector<SharedQueryPlanPtr> newSharedQueryMetaData;
    NES_TRACE("GlobalQueryPlan: Iterate over the Map of shared query metadata.");
    for (auto& [sharedQueryId, sharedQueryMetaData] : sharedQueryIdToPlanMap) {
        if (sharedQueryMetaData->isNew()) {
            newSharedQueryMetaData.emplace_back(sharedQueryMetaData);
        }
    }
    NES_TRACE("GlobalQueryPlan: Found " << newSharedQueryMetaData.size() << "  Shared Query MetaData.");
    return newSharedQueryMetaData;
}

std::vector<SharedQueryPlanPtr> GlobalQueryPlan::getAllOldSharedQueryPlans() {
    NES_INFO("GlobalQueryPlan: Get all metadata information");
    std::vector<SharedQueryPlanPtr> oldSharedQueryMetaData;
    NES_TRACE("GlobalQueryPlan: Iterate over the Map of shared query metadata.");
    for (auto& [sharedQueryId, sharedQueryMetaData] : sharedQueryIdToPlanMap) {
        if (!sharedQueryMetaData->isNew()) {
            oldSharedQueryMetaData.emplace_back(sharedQueryMetaData);
        }
    }
    NES_TRACE("GlobalQueryPlan: Found " << oldSharedQueryMetaData.size() << "  Shared Query MetaData.");
    return oldSharedQueryMetaData;
}

SharedQueryPlanPtr GlobalQueryPlan::getSharedQueryPlan(SharedQueryId sharedQueryId) {
    auto found = sharedQueryIdToPlanMap.find(sharedQueryId);
    if (found == sharedQueryIdToPlanMap.end()) {
        return nullptr;
    }
    return found->second;
}

bool GlobalQueryPlan::createNewSharedQueryPlan(const QueryPlanPtr& queryPlan) {
    NES_INFO("Create new shared query plan");
    QueryId inputQueryPlanId = queryPlan->getQueryId();
    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    SharedQueryId sharedQueryId = sharedQueryPlan->getSharedQueryId();
    queryIdToSharedQueryIdMap[inputQueryPlanId] = sharedQueryId;
    sharedQueryIdToPlanMap[sharedQueryId] = sharedQueryPlan;
    sourceNamesToSharedQueryPlanMap[queryPlan->getSourceConsumed()] = sharedQueryPlan;
    return true;
}

const std::vector<QueryPlanPtr>& GlobalQueryPlan::getQueryPlansToAdd() const { return queryPlansToAdd; }

bool GlobalQueryPlan::clearQueryPlansToAdd() {
    queryPlansToAdd.clear();
    return true;
}

SharedQueryPlanPtr GlobalQueryPlan::fetchSharedQueryPlanConsumingSources(std::string sourceNames) {
    auto item = sourceNamesToSharedQueryPlanMap.find(sourceNames);
    if (item != sourceNamesToSharedQueryPlanMap.end()) {
        return item->second;
    }
    return nullptr;
}

}// namespace NES
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
#include <Plans/Global/Query/GlobalQueryNode.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryMetaData.hpp>
#include <Plans/Query/QueryPlan.hpp>

namespace NES {

GlobalQueryPlan::GlobalQueryPlan() : freeSharedQueryId(0) {}

GlobalQueryPlanPtr GlobalQueryPlan::create() { return std::make_shared<GlobalQueryPlan>(GlobalQueryPlan()); }

bool GlobalQueryPlan::addQueryPlan(QueryPlanPtr queryPlan) {
    QueryId inputQueryPlanId = queryPlan->getQueryId();
    if (inputQueryPlanId == INVALID_QUERY_ID) {
        throw Exception("GlobalQueryPlan: Can not add query plan with invalid id.");
    }

    if (queryIdToSharedQueryIdMap.find(inputQueryPlanId) != queryIdToSharedQueryIdMap.end()) {
        throw Exception("GlobalQueryPlan: Query plan with id " + std::to_string(inputQueryPlanId) + " already present.");
    }

    auto sharedQueryMetadata = SharedQueryMetaData::create(queryPlan);
    return updateSharedQueryMetadata(sharedQueryMetadata);
}

void GlobalQueryPlan::removeQuery(QueryId queryId) {
    NES_DEBUG("GlobalQueryPlan: Removing query information from the meta data");
    SharedQueryId sharedQueryId = queryIdToSharedQueryIdMap[queryId];
    SharedQueryMetaDataPtr sharedQueryMetaData = sharedQueryIdToMetaDataMap[sharedQueryId];
    if (!sharedQueryMetaData->removeQueryId(queryId)) {
        throw Exception("GlobalQueryPlan: Unable to remove query with id " + std::to_string(queryId)
                        + " from shared query plan with id " + std::to_string(sharedQueryId));
    }
    queryIdToSharedQueryIdMap.erase(queryId);
}

std::vector<SharedQueryMetaDataPtr> GlobalQueryPlan::getSharedQueryMetaDataToDeploy() {
    NES_DEBUG("GlobalQueryPlan: Get the Global MetaData to be deployed.");
    std::vector<SharedQueryMetaDataPtr> sharedQueryMetaDataToDeploy;
    NES_TRACE("GlobalQueryPlan: Iterate over the Map with global query metadata.");
    for (auto [sharedQueryId, sharedQueryMetaData] : sharedQueryIdToMetaDataMap) {
        if (sharedQueryMetaData->isDeployed()) {
            NES_TRACE("GlobalQueryPlan: Skipping! found already deployed query meta data.");
            continue;
        }
        sharedQueryMetaDataToDeploy.push_back(sharedQueryMetaData);
    }
    NES_DEBUG("GlobalQueryPlan: Found " << sharedQueryMetaDataToDeploy.size() << "  Shared Query MetaData to be deployed.");
    return sharedQueryMetaDataToDeploy;
}

SharedQueryId GlobalQueryPlan::getSharedQueryIdForQuery(QueryId queryId) {
    NES_DEBUG("GlobalQueryPlan: Get the Global Query Id for the query " << queryId);
    if (queryIdToSharedQueryIdMap.find(queryId) != queryIdToSharedQueryIdMap.end()) {
        return queryIdToSharedQueryIdMap[queryId];
    }
    NES_WARNING("GlobalQueryPlan: Unable to find Global Query Id for the query " << queryId);
    return INVALID_SHARED_QUERY_ID;
}

bool GlobalQueryPlan::updateSharedQueryMetadata(SharedQueryMetaDataPtr sharedQueryMetaData) {
    NES_INFO("GlobalQueryPlan: updating the shared query metadata information");
    auto sharedQueryId = sharedQueryMetaData->getSharedQueryId();
    sharedQueryIdToMetaDataMap[sharedQueryId] = sharedQueryMetaData;

    NES_TRACE("GlobalQueryPlan: Updating the Query Id to Shared Query Id map");
    for (auto queryId : sharedQueryMetaData->getQueryIds()) {
        queryIdToSharedQueryIdMap[queryId] = sharedQueryId;
    }
    return true;
}

void GlobalQueryPlan::removeEmptySharedQueryMetaData() {
    NES_INFO("GlobalQueryPlan: remove empty metadata information.");
    //Following associative-container erase idiom
    for (auto itr = sharedQueryIdToMetaDataMap.begin(); itr != sharedQueryIdToMetaDataMap.end();) {
        auto sharedQueryMetaData = itr->second;
        if ((sharedQueryMetaData->isDeployed() || sharedQueryMetaData->isNew()) && sharedQueryMetaData->isEmpty()) {
            NES_TRACE("GlobalQueryPlan: Removing! found an empty query meta data.");
            sharedQueryIdToMetaDataMap.erase(itr++);
        } else {
            NES_INFO("Not Empty");
            ++itr;
        }
    }
}

std::vector<SharedQueryMetaDataPtr> GlobalQueryPlan::getAllSharedQueryMetaData() {
    NES_INFO("GlobalQueryPlan: Get all metadata information");
    std::vector<SharedQueryMetaDataPtr> sharedQueryMetaDataToDeploy;
    NES_TRACE("GlobalQueryPlan: Iterate over the Map of shared query metadata.");
    for (auto [sharedQueryId, sharedQueryMetaData] : sharedQueryIdToMetaDataMap) {
        sharedQueryMetaDataToDeploy.emplace_back(sharedQueryMetaData);
    }
    NES_TRACE("GlobalQueryPlan: Found " << sharedQueryMetaDataToDeploy.size() << "  Shared Query MetaData.");
    return sharedQueryMetaDataToDeploy;
}

SharedQueryMetaDataPtr GlobalQueryPlan::getSharedQueryMetaData(SharedQueryId sharedQueryId) {
    auto found = sharedQueryIdToMetaDataMap.find(sharedQueryId);
    if (found == sharedQueryIdToMetaDataMap.end()) {
        return nullptr;
    }
    return found->second;
}
}// namespace NES
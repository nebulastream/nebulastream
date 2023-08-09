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

#include <Exceptions/QueryNotFoundException.hpp>
#include <Exceptions/GlobalQueryPlanUpdateException.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/OperatorNode.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/magicenum/magic_enum.hpp>

namespace NES {

GlobalQueryPlan::GlobalQueryPlan() = default;

GlobalQueryPlanPtr GlobalQueryPlan::create() { return std::make_shared<GlobalQueryPlan>(GlobalQueryPlan()); }

void GlobalQueryPlan::addQueryPlan(const QueryPlanPtr& queryPlan) {
    QueryId inputQueryPlanId = queryPlan->getQueryId();
    if (inputQueryPlanId == INVALID_QUERY_ID) {
        throw Exceptions::QueryNotFoundException("GlobalQueryPlan: Can not add query plan with invalid id.");
    }
    if (queryIdToSharedQueryIdMap.find(inputQueryPlanId) != queryIdToSharedQueryIdMap.end()) {
        throw GlobalQueryPlanUpdateException("GlobalQueryPlan: Query plan with id "
                                                           + std::to_string(inputQueryPlanId) + " already present.");
    }
    queryPlansToAdd.emplace_back(queryPlan);
}

void GlobalQueryPlan::removeQuery(QueryId queryId, RequestType requestType) {
    NES_DEBUG("GlobalQueryPlan: Removing query information from the meta data");

    if (RequestType::FailQuery == requestType) {
        //For failure request query id is nothing but id of the shared query plan
        auto sharedQueryPlan = sharedQueryIdToPlanMap[queryId];
        //Instead of removing query we mark the status of the shared query plan to failed
        sharedQueryPlan->setStatus(SharedQueryPlanStatus::Failed);
    } else if (RequestType::StopQuery == requestType) {
        //Check if the query id present in the Global query Plan
        if (queryIdToSharedQueryIdMap.find(queryId) != queryIdToSharedQueryIdMap.end()) {
            //Fetch the shared query plan id and remove the query and associated operators
            SharedQueryId sharedQueryId = queryIdToSharedQueryIdMap[queryId];
            SharedQueryPlanPtr sharedQueryPlan = sharedQueryIdToPlanMap[sharedQueryId];
            if (!sharedQueryPlan->removeQuery(queryId)) {
                //todo: #3821 create specific exception for this case
                throw Exceptions::RuntimeException("GlobalQueryPlan: Unable to remove query with id " + std::to_string(queryId)
                                                   + " from shared query plan with id " + std::to_string(sharedQueryId));
            }

            if (sharedQueryPlan->isEmpty()) {
                // Mark SQP as stopped if all queries are removed post stop
                sharedQueryPlan->setStatus(SharedQueryPlanStatus::Stopped);
            } else {
                // Mark SQP as updated if after stop more queries are remaining
                sharedQueryPlan->setStatus(SharedQueryPlanStatus::Updated);
            }

            //Remove from the queryId to shared query id map
            queryIdToSharedQueryIdMap.erase(queryId);
        } else {
            // Check if the query is in the list of query plans to add and then remove it
            queryPlansToAdd.erase(
                std::find_if(queryPlansToAdd.begin(), queryPlansToAdd.end(), [&queryId](const QueryPlanPtr& queryPlan) {
                    return queryPlan->getQueryId() == queryId;
                }));
        }
    } else {
        NES_ERROR("Unknown request type {}", std::string(magic_enum::enum_name(requestType)));
        NES_NOT_IMPLEMENTED();
    }
}

std::vector<SharedQueryPlanPtr> GlobalQueryPlan::getSharedQueryPlansToDeploy() {
    NES_DEBUG("GlobalQueryPlan: Get the shared query plans to deploy.");
    std::vector<SharedQueryPlanPtr> sharedQueryPlansToDeploy;
    for (auto& [sharedQueryId, sharedQueryPlan] : sharedQueryIdToPlanMap) {
        if (SharedQueryPlanStatus::Deployed == sharedQueryPlan->getStatus()) {
            NES_TRACE("GlobalQueryPlan: Skipping! found already deployed shared query plan.");
            continue;
        }
        sharedQueryPlansToDeploy.push_back(sharedQueryPlan);
    }
    NES_DEBUG("GlobalQueryPlan: Found {} Shared Query plan to be deployed.", sharedQueryPlansToDeploy.size());
    return sharedQueryPlansToDeploy;
}

SharedQueryId GlobalQueryPlan::getSharedQueryId(QueryId queryId) {
    NES_TRACE("GlobalQueryPlan: Get the Global Query Id for the query  {}", queryId);
    if (queryIdToSharedQueryIdMap.find(queryId) != queryIdToSharedQueryIdMap.end()) {
        return queryIdToSharedQueryIdMap[queryId];
    }
    NES_TRACE("GlobalQueryPlan: Unable to find Global Query Id for the query  {}", queryId);
    return INVALID_SHARED_QUERY_ID;
}

bool GlobalQueryPlan::updateSharedQueryPlan(const SharedQueryPlanPtr& sharedQueryPlan) {
    NES_INFO("GlobalQueryPlan: updating the shared query metadata information");
    auto sharedQueryId = sharedQueryPlan->getId();
    //Mark the shared query plan as updated post merging new queries
    sharedQueryPlan->setStatus(SharedQueryPlanStatus::Updated);
    sharedQueryIdToPlanMap[sharedQueryId] = sharedQueryPlan;
    NES_TRACE("GlobalQueryPlan: Updating the Query Id to Shared Query Id map");
    for (auto queryId : sharedQueryPlan->getQueryIds()) {
        queryIdToSharedQueryIdMap[queryId] = sharedQueryId;
    }
    return true;
}

void GlobalQueryPlan::removeFailedOrStoppedSharedQueryPlans() {
    NES_INFO("GlobalQueryPlan: remove empty metadata information.");
    //Following associative-container erase idiom
    for (auto itr = sharedQueryIdToPlanMap.begin(); itr != sharedQueryIdToPlanMap.end();) {
        auto sharedQueryPlan = itr->second;
        //Remove all plans that are stopped or Failed
        if (sharedQueryPlan->getStatus() == SharedQueryPlanStatus::Failed
            || sharedQueryPlan->getStatus() == SharedQueryPlanStatus::Stopped) {
            NES_TRACE("GlobalQueryPlan: Removing! found an empty query meta data.");
            sharedQueryIdToPlanMap.erase(itr++);
            continue;
        }
        itr++;
    }
}

void GlobalQueryPlan::removeSharedQueryPlan(QueryId sharedQueryPlanId) {
    NES_INFO("GlobalQueryPlan: remove metadata information for empty shared query plan id {}", sharedQueryPlanId);
    if (sharedQueryPlanId == INVALID_SHARED_QUERY_ID) {
        throw Exceptions::RuntimeException("GlobalQueryPlan: Cannot remove shared query plan with invalid id.");
    }
    auto sharedQueryPlan = sharedQueryIdToPlanMap[sharedQueryPlanId];
    if (sharedQueryPlan->getStatus() == SharedQueryPlanStatus::Stopped
        || sharedQueryPlan->getStatus() == SharedQueryPlanStatus::Failed) {
        NES_TRACE("Found stopped or failed query plan. Removing query plan from shared query plan.");
        sharedQueryIdToPlanMap.erase(sharedQueryPlanId);
    }
}

std::vector<SharedQueryPlanPtr> GlobalQueryPlan::getAllSharedQueryPlans() {
    NES_INFO("GlobalQueryPlan: Get all metadata information");
    std::vector<SharedQueryPlanPtr> sharedQueryPlans;
    sharedQueryPlans.reserve(sharedQueryIdToPlanMap.size());
    NES_TRACE("GlobalQueryPlan: Iterate over the Map of shared query metadata.");
    for (auto& [sharedQueryId, sharedQueryPlan] : sharedQueryIdToPlanMap) {
        sharedQueryPlans.emplace_back(sharedQueryPlan);
    }
    NES_TRACE("GlobalQueryPlan: Found {} Shared Query MetaData.", sharedQueryPlans.size());
    return sharedQueryPlans;
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
    SharedQueryId sharedQueryId = sharedQueryPlan->getId();
    queryIdToSharedQueryIdMap[inputQueryPlanId] = sharedQueryId;
    sharedQueryIdToPlanMap[sharedQueryId] = sharedQueryPlan;
    std::string sourceNameAndPlacementStrategy =
        queryPlan->getSourceConsumed() + "_" + std::to_string(magic_enum::enum_integer(queryPlan->getPlacementStrategy()));
    //Add Shared Query Plan to the SourceName index
    auto item = sourceNamesAndPlacementStrategyToSharedQueryPlanMap.find(sourceNameAndPlacementStrategy);
    if (item != sourceNamesAndPlacementStrategyToSharedQueryPlanMap.end()) {
        auto sharedQueryPlans = item->second;
        sharedQueryPlans.emplace_back(sharedQueryPlan);
        sourceNamesAndPlacementStrategyToSharedQueryPlanMap[sourceNameAndPlacementStrategy] = sharedQueryPlans;
    } else {
        sourceNamesAndPlacementStrategyToSharedQueryPlanMap[sourceNameAndPlacementStrategy] = {sharedQueryPlan};
    }

    return true;
}

const std::vector<QueryPlanPtr>& GlobalQueryPlan::getQueryPlansToAdd() const { return queryPlansToAdd; }

bool GlobalQueryPlan::clearQueryPlansToAdd() {
    queryPlansToAdd.clear();
    return true;
}

std::vector<SharedQueryPlanPtr>
GlobalQueryPlan::getSharedQueryPlansConsumingSourcesAndPlacementStrategy(const std::string& sourceNames,
                                                                         Optimizer::PlacementStrategy placementStrategy) {
    std::string sourceNameAndPlacementStrategy = sourceNames + "_" + std::to_string(magic_enum::enum_integer(placementStrategy));
    auto item = sourceNamesAndPlacementStrategyToSharedQueryPlanMap.find(sourceNameAndPlacementStrategy);
    if (item != sourceNamesAndPlacementStrategyToSharedQueryPlanMap.end()) {
        return item->second;
    }
    return {};
}

std::vector<QueryId> GlobalQueryPlan::getQueryIds(SharedQueryId sharedQueryPlanId) {
    NES_TRACE("Fetch query ids associated to the shared query plan id");
    auto sharedQueryPlan = getSharedQueryPlan(sharedQueryPlanId);
    return sharedQueryPlan->getQueryIds();
}

}// namespace NES
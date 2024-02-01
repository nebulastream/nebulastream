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

#include <Catalogs/Exceptions/InvalidQueryStateException.hpp>
#include <Catalogs/Exceptions/QueryNotFoundException.hpp>
#include <Catalogs/Query/DecomposedQueryPlanMetaData.hpp>
#include <Catalogs/Query/QueryCatalog.hpp>
#include <Catalogs/Query/SharedQueryCatalogEntry.hpp>
#include <Exceptions/InvalidArgumentException.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <string>

namespace NES::Catalogs::Query {

void QueryCatalog::createQueryCatalogEntry(const std::string& queryString,
                                           const NES::QueryPlanPtr& queryPlan,
                                           const Optimizer::PlacementStrategy placementStrategyName,
                                           QueryState queryState) {
    auto queryId = queryPlan->getQueryId();
    NES_INFO("Create query catalog entry for the query with id {}", queryId);
    auto queryCatalogEntry =
        std::make_shared<QueryCatalogEntry>(queryId, queryString, placementStrategyName, queryPlan, queryState);
    auto lockedQueryCatalogEntryMapping = queryCatalogEntryMapping.wlock();
    (*lockedQueryCatalogEntryMapping)[queryId] = std::move(queryCatalogEntry);
}

void QueryCatalog::createSharedQueryCatalogEntry(SharedQueryId sharedQueryId,
                                                 std::vector<QueryId> queryIds,
                                                 QueryState queryStatus) {
    NES_INFO("Create shared query catalog entry for the shared query with id {}", sharedQueryId);
    auto sharedQueryCatalogEntry = std::make_shared<SharedQueryCatalogEntry>(sharedQueryId, queryIds, queryStatus);
    auto lockedSharedQueryCatalogEntryMapping = sharedQueryCatalogEntryMapping.wlock();
    (*lockedSharedQueryCatalogEntryMapping)[sharedQueryId] = std::move(sharedQueryCatalogEntry);
}

bool QueryCatalog::updateQueryStatus(QueryId queryId, QueryState queryStatus, const std::string& metaInformation) {

    auto lockedQueryCatalogEntryMapping = queryCatalogEntryMapping.wlock();
    //Check if query exists
    if (!lockedQueryCatalogEntryMapping->contains(queryId)) {
        NES_ERROR("QueryCatalogService: Query Catalog does not contains the input queryId {}", std::to_string(queryId));
        throw Exceptions::QueryNotFoundException("Query Catalog does not contains the input queryId " + std::to_string(queryId));
    }

    auto queryCatalogEntry = (*lockedQueryCatalogEntryMapping)[queryId];
    //Handle new status of the query
    switch (queryStatus) {
        case QueryState::REGISTERED:
        case QueryState::OPTIMIZING:
        case QueryState::RESTARTING:
        case QueryState::MIGRATING:
        case QueryState::DEPLOYED:
        case QueryState::STOPPED:
        case QueryState::RUNNING:
        case QueryState::FAILED: {
            queryCatalogEntry->setQueryState(queryStatus);
            queryCatalogEntry->setMetaInformation(metaInformation);
            return true;
        }
        default:
            throw Exceptions::InvalidQueryStateException({QueryState::REGISTERED,
                                                          QueryState::OPTIMIZING,
                                                          QueryState::RESTARTING,
                                                          QueryState::MIGRATING,
                                                          QueryState::STOPPED,
                                                          QueryState::RUNNING,
                                                          QueryState::FAILED},
                                                         queryStatus);
    }
}

bool QueryCatalog::updateSharedQueryStatus(SharedQueryId sharedQueryId,
                                           QueryState queryState,
                                           const std::string& metaInformation) {

    auto lockedSharedQueryCatalogEntryMapping = sharedQueryCatalogEntryMapping.wlock();
    if (!lockedSharedQueryCatalogEntryMapping->contains(sharedQueryId)) {
        NES_ERROR("QueryCatalogService: Query Catalog does not contains the input queryId {}", std::to_string(sharedQueryId));
        throw Exceptions::QueryNotFoundException("Query Catalog does not contains the input queryId "
                                                 + std::to_string(sharedQueryId));
    }

    auto sharedQueryCatalogEntry = (*lockedSharedQueryCatalogEntryMapping)[sharedQueryId];

    //Handle new status of the query
    switch (queryState) {
        case QueryState::OPTIMIZING:
        case QueryState::RESTARTING:
        case QueryState::MIGRATING:
        case QueryState::DEPLOYED:
        case QueryState::STOPPED:
        case QueryState::RUNNING:
        case QueryState::FAILED: {
            sharedQueryCatalogEntry->setQueryState(queryState);
            sharedQueryCatalogEntry->setTerminationReason(metaInformation);
            for (const auto& decomposedQueryPlanMetaData : sharedQueryCatalogEntry->getAllDecomposedQueryPlanMetaData()) {
                decomposedQueryPlanMetaData->updateState(queryState);
            }
            return true;
        }
        default:
            throw Exceptions::InvalidQueryStateException({QueryState::OPTIMIZING,
                                                          QueryState::RESTARTING,
                                                          QueryState::MIGRATING,
                                                          QueryState::DEPLOYED,
                                                          QueryState::STOPPED,
                                                          QueryState::RUNNING,
                                                          QueryState::FAILED},
                                                         queryState);
    }
}

bool QueryCatalog::checkAndMarkSharedQueryForSoftStop(SharedQueryId sharedQueryId,
                                                      DecomposedQueryPlanId decomposedQueryPlanId,
                                                      OperatorId operatorId) {

    NES_INFO("checkAndMarkForSoftStop sharedQueryId={} subQueryId={} source={}",
             sharedQueryId,
             decomposedQueryPlanId,
             operatorId);
    //Fetch shared query and query catalogs
    auto [lockedSharedQueryCatalogEntryMapping, lockedQueryCatalogEntryMapping] =
        folly::acquireLocked(sharedQueryCatalogEntryMapping, queryCatalogEntryMapping);
    if (!lockedSharedQueryCatalogEntryMapping->contains(sharedQueryId)) {
        NES_ERROR("QueryCatalogService: Query Catalog does not contains the input queryId {}", std::to_string(sharedQueryId));
        throw Exceptions::QueryNotFoundException("Query Catalog does not contains the input queryId "
                                                 + std::to_string(sharedQueryId));
    }

    auto sharedQueryCatalogEntry = (*lockedSharedQueryCatalogEntryMapping)[sharedQueryId];
    auto containedQueryIds = sharedQueryCatalogEntry->getContainedQueryIds();

    //If query is doing hard stop or has failed or already stopped then soft stop can not be triggered
    auto currentSharedQueryState = sharedQueryCatalogEntry->getQueryState();
    if (currentSharedQueryState == QueryState::MARKED_FOR_HARD_STOP || currentSharedQueryState == QueryState::FAILED
        || currentSharedQueryState == QueryState::STOPPED) {
        NES_WARNING("QueryCatalogService: Soft stop can not be initiated as query in {} status.",
                    magic_enum::enum_name(currentSharedQueryState));
        return false;
    }

    //Mark queries for soft stop and return
    for (const auto& containedQueryId : containedQueryIds) {
        auto queryCatalogEntry = (*lockedQueryCatalogEntryMapping)[containedQueryId];
        queryCatalogEntry->setQueryState(QueryState::MARKED_FOR_SOFT_STOP);
    }

    //Mark shared query for soft stop and return
    sharedQueryCatalogEntry->setQueryState(QueryState::MARKED_FOR_SOFT_STOP);
    NES_INFO("QueryCatalogService: Shared query id {} is marked as soft stopped", sharedQueryId);
    return true;
}

bool QueryCatalog::checkAndMarkQueryForHardStop(QueryId queryId) {

    NES_INFO("QueryCatalogService: Handle hard stop request.");

    //Fetch shared query and query catalogs
    auto lockedQueryCatalogEntryMapping = queryCatalogEntryMapping.wlock();

    //Check if query exists
    if (!lockedQueryCatalogEntryMapping->contains(queryId)) {
        NES_ERROR("QueryCatalogService: Query Catalog does not contains the input queryId {}", std::to_string(queryId));
        throw Exceptions::QueryNotFoundException("Query Catalog does not contains the input queryId " + std::to_string(queryId));
    }
    auto queryCatalogEntry = (*lockedQueryCatalogEntryMapping)[queryId];
    auto currentStatus = queryCatalogEntry->getQueryState();
    auto sharedQueryId = queryCatalogEntry->getSharedQueryId();
    if (currentStatus == QueryState::MARKED_FOR_SOFT_STOP || currentStatus == QueryState::MARKED_FOR_HARD_STOP
        || currentStatus == QueryState::MARKED_FOR_FAILURE || currentStatus == QueryState::DEPLOYED
        || currentStatus == QueryState::STOPPED || currentStatus == QueryState::FAILED) {
        NES_ERROR("QueryCatalog: Found query status already as {}. Ignoring stop query request.",
                  queryCatalogEntry->getQueryStatusAsString());
        return false;
    }
    NES_DEBUG("QueryCatalog: Changing query status to Mark query for stop.");
    queryCatalogEntry->setQueryState(QueryState::MARKED_FOR_HARD_STOP);
    return true;
}

void QueryCatalog::checkAndMarkSharedQueryForFailure(SharedQueryId sharedQueryId, DecomposedQueryPlanId querySubPlanId) {

    NES_INFO("checkAndMarkForFailure sharedQueryId={} subQueryId={}", sharedQueryId, querySubPlanId);
    //Fetch query catalog entries
    auto queryCatalogEntries = queryCatalog->getQueryCatalogEntriesForSharedQueryId(sharedQueryId);

    if (queryCatalogEntries.empty()) {
        NES_FATAL_ERROR("Unable to find the shared query plan with id {}", sharedQueryId);
        throw Exceptions::QueryNotFoundException("Unable to find the shared query plan with id " + std::to_string(sharedQueryId));
    }

    // First perform a check if query can be marked for stop
    for (auto& queryCatalogEntry : queryCatalogEntries) {
        //If query is doing hard stop or has failed or already stopped then soft stop can not be triggered
        QueryState currentQueryStatus = queryCatalogEntry->getQueryState();
        if (currentQueryStatus == QueryState::MARKED_FOR_FAILURE || currentQueryStatus == QueryState::FAILED
            || currentQueryStatus == QueryState::STOPPED) {
            NES_WARNING("QueryCatalogService: Query can not be marked for failure as query in {} status.",
                        queryCatalogEntry->getQueryStatusAsString());
            throw Exceptions::InvalidQueryStateException({QueryState::REGISTERED,
                                                          QueryState::OPTIMIZING,
                                                          QueryState::DEPLOYED,
                                                          QueryState::RUNNING,
                                                          QueryState::MARKED_FOR_HARD_STOP,
                                                          QueryState::MARKED_FOR_SOFT_STOP,
                                                          QueryState::SOFT_STOP_TRIGGERED,
                                                          QueryState::SOFT_STOP_COMPLETED,
                                                          QueryState::RESTARTING,
                                                          QueryState::MIGRATING},
                                                         currentQueryStatus);
        }
    }

    //Mark queries for failure and return
    for (auto& queryCatalogEntry : queryCatalogEntries) {
        queryCatalogEntry->setQueryState(QueryState::MARKED_FOR_FAILURE);
        for (const auto& subQueryPlanMetaData : queryCatalogEntry->getAllSubQueryPlanMetaData()) {
            //Mark the sub query plan as already failed for which the failure message was received
            if (subQueryPlanMetaData->getDecomposedQueryPlanId() == querySubPlanId) {
                subQueryPlanMetaData->updateState(QueryState::FAILED);
            } else {
                subQueryPlanMetaData->updateState(QueryState::MARKED_FOR_FAILURE);
            }
        }
    }
    NES_INFO("QueryCatalogService: Shared query id {} is marked as failed", sharedQueryId);
}

void QueryCatalog::addDecomposedQueryMetaData(SharedQueryId sharedQueryId,
                                              DecomposedQueryPlanId decomposedQueryPlanId,
                                              WorkerId workerId,
                                              QueryState querySubPlanStatus) {
    //Check if query exists
    if (!queryCatalog->queryExists(sharedQueryId)) {
        NES_ERROR("QueryCatalogService: Query Catalog does not contains the input queryId {}", std::to_string(sharedQueryId));
        throw Exceptions::QueryNotFoundException("Query Catalog does not contains the input queryId "
                                                 + std::to_string(sharedQueryId));
    }

    //Fetch the current entry for the query
    auto queryEntry = queryCatalog->getQueryCatalogEntry(sharedQueryId);
    //Add new query sub plan
    queryEntry->addQuerySubPlanMetaData(decomposedQueryPlanId, workerId, querySubPlanStatus);
}

bool QueryCatalog::handleSoftStop(SharedQueryId sharedQueryId,
                                  DecomposedQueryPlanId decomposedQueryPlanId,
                                  QueryState queryState) {
    NES_DEBUG("QueryCatalogService: Updating the status of sub query to ({}) for sub query plan with id {} for shared query "
              "plan with id {}",
              std::string(magic_enum::enum_name(queryState)),
              decomposedQueryPlanId,
              sharedQueryId);

    //Fetch query catalog entries
    auto queryCatalogEntries = queryCatalog->getQueryCatalogEntriesForSharedQueryId(sharedQueryId);
    for (auto& queryCatalogEntry : queryCatalogEntries) {
        auto queryId = queryCatalogEntry->getQueryId();
        //Check if query is in correct status
        auto currentQueryStatus = queryCatalogEntry->getQueryState();
        //todo #4396: when query migration has its own termination type, this function will not be called during migration. remove MIGRATING below
        if (currentQueryStatus != QueryState::MARKED_FOR_SOFT_STOP && currentQueryStatus != QueryState::MIGRATING) {
            NES_WARNING("Found query in {} but received {} for the sub query with id {} for query id {}",
                        queryCatalogEntry->getQueryStatusAsString(),
                        std::string(magic_enum::enum_name(querySubPlanStatus)),
                        decomposedQueryPlanId,
                        queryId);
            //FIXME: fix what to do when this occurs
            NES_ASSERT(false,
                       "Found query in " << queryCatalogEntry->getQueryStatusAsString() << " but received "
                                         << std::string(magic_enum::enum_name(querySubPlanStatus))
                                         << " for the sub query with id " << decomposedQueryPlanId << " for query id "
                                         << queryId);
        }

        //Get the sub query plan
        auto querySubPlanMetaData = queryCatalogEntry->getQuerySubPlanMetaData(decomposedQueryPlanId);

        // check the query sub plan status
        auto currentStatus = querySubPlanMetaData->getDecomposedQueryPlanStatus();
        if (currentStatus == QueryState::SOFT_STOP_COMPLETED && querySubPlanStatus == QueryState::SOFT_STOP_COMPLETED) {
            NES_WARNING("Received multiple soft stop completed for sub query with id {} for query {}",
                        decomposedQueryPlanId,
                        queryId);
            NES_WARNING("Skipping remaining operation");
            continue;
        } else if (currentStatus == QueryState::SOFT_STOP_COMPLETED && querySubPlanStatus == QueryState::SOFT_STOP_TRIGGERED) {
            NES_ERROR("Received soft stop triggered for sub query with id {} for query {} but sub query is already marked as "
                      "soft stop completed.",
                      decomposedQueryPlanId,
                      sharedQueryId);
            NES_WARNING("Skipping remaining operation");
            continue;
        } else if (currentStatus == QueryState::SOFT_STOP_TRIGGERED && querySubPlanStatus == QueryState::SOFT_STOP_TRIGGERED) {
            NES_ERROR("Received multiple soft stop triggered for sub query with id {} for query {}",
                      decomposedQueryPlanId,
                      sharedQueryId);
            NES_WARNING("Skipping remaining operation");
            continue;
        }

        querySubPlanMetaData->updateState(querySubPlanStatus);

        if (currentQueryStatus == QueryState::MIGRATING) {
            if (querySubPlanStatus == QueryState::SOFT_STOP_COMPLETED) {
                /* receiving a soft stop for a query sub plan of a migrating query marks the completion of the
                 * migration of that specific query sub plan.*/
                if (querySubPlanStatus == QueryState::SOFT_STOP_COMPLETED) {
                    //todo #4396: once a specifig drain EOS is implemented, remove this as the received state should already equal migration completed
                    querySubPlanMetaData->updateState(QueryState::MIGRATION_COMPLETED);
                }

                /*if the query is in MIGRATING status, check if there are still any querySubPlans that did not finish their
                 * migration. If no subplan in state MIGRATING could be found, the migration of the whole query is complete
                 * and the state is set to RUNNING again */
                bool queryMigrationComplete = true;
                for (auto& querySubPlanMetaData : queryCatalogEntry->getAllSubQueryPlanMetaData()) {
                    auto querySubPlanStatus = querySubPlanMetaData->getDecomposedQueryPlanStatus();
                    if (querySubPlanStatus == QueryState::MIGRATING) {
                        queryMigrationComplete = false;
                        break;
                    }
                    NES_ASSERT(querySubPlanStatus == QueryState::RUNNING || querySubPlanStatus == QueryState::SOFT_STOP_COMPLETED
                                   || querySubPlanStatus == QueryState::MIGRATION_COMPLETED
                                   || querySubPlanStatus == QueryState::REDEPLOYED,
                               "Unexpected subplan status.");
                }
                if (queryMigrationComplete) {
                    queryCatalogEntry->setQueryState(QueryState::RUNNING);
                }
            }
        } else {
            //Check if all sub queryIdAndCatalogEntryMapping are stopped when a sub query soft stop completes
            bool stopQuery = true;
            if (querySubPlanStatus == QueryState::SOFT_STOP_COMPLETED) {
                for (auto& querySubPlanMetaData : queryCatalogEntry->getAllSubQueryPlanMetaData()) {
                    NES_DEBUG("Updating query subplan status for query id= {} subplan= {} is {}",
                              queryId,
                              querySubPlanMetaData->getDecomposedQueryPlanId(),
                              std::string(magic_enum::enum_name(querySubPlanMetaData->getDecomposedQueryPlanStatus())));
                    if (querySubPlanMetaData->getDecomposedQueryPlanStatus() != QueryState::SOFT_STOP_COMPLETED) {
                        stopQuery = false;
                        break;
                    }
                }
                // Mark the query as stopped if all sub queryIdAndCatalogEntryMapping are stopped
                if (stopQuery) {
                    queryCatalogEntry->setQueryState(QueryState::STOPPED);
                    NES_INFO("Query with id {} is now stopped", queryCatalogEntry->getQueryId());
                }
            }
        }
    }
    return true;
}

bool QueryCatalog::updateDecomposedQueryPlanStatus(SharedQueryId sharedQueryId,
                                                   DecomposedQueryPlanId decomposedQueryPlanId,
                                                   DecomposedQueryPlanVersion decomposedQueryPlanVersion,
                                                   QueryState queryState) {

    switch (queryState) {
        case QueryState::MARKED_FOR_DEPLOYMENT:
        case QueryState::MARKED_FOR_REDEPLOYMENT:
        case QueryState::MARKED_FOR_MIGRATION: {
            updateDecomposedQueryPlanStatus(sharedQueryId, decomposedQueryPlanId, querySubPlanStatus);
            break;
        }
        case QueryState::SOFT_STOP_TRIGGERED:
        case QueryState::SOFT_STOP_COMPLETED: {
            handleSoftStop(sharedQueryId, decomposedQueryPlanId, querySubPlanStatus);
            break;
        }
        default:
            throw Exceptions::InvalidQueryStateException({QueryState::SOFT_STOP_TRIGGERED, QueryState::SOFT_STOP_COMPLETED},
                                                         querySubPlanStatus);
    }
    return true;
}

void QueryCatalog::addUpdatedQueryPlan(QueryId queryId, std::string step, QueryPlanPtr updatedQueryPlan) {

    //Check if query exists
    if (!queryCatalog->queryExists(queryId)) {
        NES_ERROR("QueryCatalogService: Query Catalog does not contains the input queryId {}", std::to_string(queryId));
        throw Exceptions::QueryNotFoundException("Query Catalog does not contains the input queryId " + std::to_string(queryId));
    }

    auto queryCatalogEntry = queryCatalog->getQueryCatalogEntry(queryId);
    queryCatalogEntry->addOptimizationPhase(step, updatedQueryPlan);

    if (step == "Executed Query Plan") {
        queryCatalogEntry->setExecutedQueryPlan(updatedQueryPlan);
    }
}

std::map<uint64_t, std::string> QueryCatalog::getQueriesWithStatus(const std::string& queryState) {

    auto state = magic_enum::enum_cast<QueryState>(queryState);
    if (state.has_value()) {
        NES_INFO("QueryCatalog : fetching all queryIdAndCatalogEntryMapping with status {}", queryState);
        std::map<uint64_t, QueryCatalogEntryPtr> queries = getQueryCatalogEntries(state);
        std::map<uint64_t, std::string> result;
        for (auto const& [key, value] : queries) {
            result[key] = value->getQueryString();
        }
        NES_INFO("QueryCatalog : found {} all queryIdAndCatalogEntryMapping with status {}", result.size(), queryState);
        return result;
    } else {
        NES_ERROR("No valid query status to parse");
        throw InvalidArgumentException("QueryState", state);
    }
}

std::map<uint64_t, std::string> QueryCatalog::getAllQueries() {
    NES_INFO("QueryCatalog : get all queryIdAndCatalogEntryMapping");
    std::map<uint64_t, QueryCatalogEntryPtr> registeredQueries = getAllQueryCatalogEntries();
    std::map<uint64_t, std::string> result;
    for (auto [key, value] : registeredQueries) {
        result[key] = value->getQueryString();
    }
    NES_INFO("QueryCatalog : found {} queryIdAndCatalogEntryMapping in catalog.", result.size());
    return result;
}

QueryCatalogEntryPtr QueryCatalog::createNewEntry(const std::string& queryString,
                                                  const QueryPlanPtr& queryPlan,
                                                  const Optimizer::PlacementStrategy placementStrategyName) {
    QueryId queryId = queryPlan->getQueryId();
    NES_INFO("QueryCatalog: Creating query catalog entry for query with id {}", queryId);
    auto queryCatalogEntry =
        std::make_shared<QueryCatalogEntry>(queryId, queryString, placementStrategyName, queryPlan, QueryState::REGISTERED);
    queryCatalogEntryMapping[queryId] = queryCatalogEntry;
    return queryCatalogEntry;
}

std::map<uint64_t, QueryCatalogEntryPtr> QueryCatalog::getAllQueryCatalogEntries() {
    NES_TRACE("QueryCatalog: return registered queryIdAndCatalogEntryMapping={}", printQueries());
    return queryCatalogEntryMapping;
}

QueryCatalogEntryPtr QueryCatalog::getQueryCatalogEntry(QueryId queryId) {
    NES_TRACE("QueryCatalog: getQueryCatalogEntry with id {}", queryId);
    return queryCatalogEntryMapping[queryId];
}

bool QueryCatalog::queryExists(QueryId queryId) {
    NES_TRACE("QueryCatalog: Check if query with id {} exists.", queryId);
    auto lockedQueryCatalogEntryMapping = queryCatalogEntryMapping.wlock();
    if (lockedQueryCatalogEntryMapping->contains(queryId)) {
        NES_TRACE("QueryCatalog: query with id {} exists.", queryId);
        return true;
    }
    NES_WARNING("QueryCatalog: query with id {} does not exist", queryId);
    return false;
}

std::map<uint64_t, QueryCatalogEntryPtr> QueryCatalog::getQueryCatalogEntries(QueryState requestedStatus) {
    NES_TRACE("QueryCatalog: getQueriesWithStatus() registered queryIdAndCatalogEntryMapping={}", printQueries());
    std::map<uint64_t, QueryCatalogEntryPtr> matchingQueries;
    for (auto const& q : queryCatalogEntryMapping) {
        if (q.second->getQueryState() == requestedStatus) {
            matchingQueries.insert(q);
        }
    }
    return matchingQueries;
}

void QueryCatalog::mapSharedQueryPlanId(SharedQueryId sharedQueryId, QueryCatalogEntryPtr queryCatalogEntry) {
    // Find the shared query plan for mapping
    if (sharedQueryCatalogEntryMapping.find(sharedQueryId) == sharedQueryCatalogEntryMapping.end()) {
        sharedQueryCatalogEntryMapping[sharedQueryId] = {queryCatalogEntry};
        return;
    }

    // Add the shared query id
    auto queryCatalogEntries = sharedQueryCatalogEntryMapping[sharedQueryId];
    queryCatalogEntries.emplace_back(queryCatalogEntry);
    sharedQueryCatalogEntryMapping[sharedQueryId] = queryCatalogEntries;
}

std::vector<QueryCatalogEntryPtr> QueryCatalog::getQueryCatalogEntriesForSharedQueryId(SharedQueryId sharedQueryId) {
    if (sharedQueryCatalogEntryMapping.find(sharedQueryId) == sharedQueryCatalogEntryMapping.end()) {
        NES_ERROR("QueryCatalog: Unable to find shared query plan with id {}", std::to_string(sharedQueryId));
        throw Exceptions::QueryNotFoundException("QueryCatalog: Unable to find shared query plan with id "
                                                 + std::to_string(sharedQueryId));
    }
    return sharedQueryCatalogEntryMapping[sharedQueryId];
}

void QueryCatalog::removeSharedQueryPlanIdMappings(SharedQueryId sharedQueryId) {
    if (sharedQueryCatalogEntryMapping.find(sharedQueryId) == sharedQueryCatalogEntryMapping.end()) {
        NES_ERROR("QueryCatalog: Unable to find shared query plan with id {}", std::to_string(sharedQueryId));
        throw Exceptions::QueryNotFoundException("QueryCatalog: Unable to find shared query plan with id "
                                                 + std::to_string(sharedQueryId));
    }
    sharedQueryCatalogEntryMapping.erase(sharedQueryId);
}

void QueryCatalog::resetCatalog() {
    NES_TRACE("QueryCatalog: clear query catalog");
    queryCatalogEntryMapping->clear();
    sharedQueryCatalogEntryMapping->clear();
}

}// namespace NES::Catalogs::Query

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

bool QueryCatalog::updateQueryStatus(QueryId queryId, QueryState queryStatus, const std::string& terminationReason) {

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
            queryCatalogEntry->setMetaInformation(terminationReason);
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
                                           const std::string& terminationReason) {

    auto lockedSharedQueryCatalogEntryMapping = sharedQueryCatalogEntryMapping.wlock();
    if (!lockedSharedQueryCatalogEntryMapping->contains(sharedQueryId)) {
        NES_ERROR("QueryCatalogService: Query Catalog does not contains the input queryId {}", std::to_string(sharedQueryId));
        throw Exceptions::QueryNotFoundException("Query Catalog does not contains the input queryId "
                                                 + std::to_string(sharedQueryId));
    }

    auto sharedQueryCatalogEntry = (*lockedSharedQueryCatalogEntryMapping)[sharedQueryId];

    //Handle new status of the query
    switch (queryState) {
        case QueryState::RESTARTING:
        case QueryState::DEPLOYED:
        case QueryState::STOPPED:
        case QueryState::RUNNING: {
            sharedQueryCatalogEntry->setQueryState(queryState);
            return true;
        }
        case QueryState::FAILED: {
            sharedQueryCatalogEntry->setQueryState(queryState);
            sharedQueryCatalogEntry->setTerminationReason(terminationReason);
            for (const auto& decomposedQueryPlanMetaData : sharedQueryCatalogEntry->getAllDecomposedQueryPlanMetaData()) {
                decomposedQueryPlanMetaData->updateState(queryState);
            }
            return true;
        }
        default:
            throw Exceptions::InvalidQueryStateException(
                {QueryState::RESTARTING, QueryState::DEPLOYED, QueryState::STOPPED, QueryState::RUNNING, QueryState::FAILED},
                queryState);
    }
}

bool QueryCatalog::updateDecomposedQueryStatus(SharedQueryId sharedQueryId,
                                               DecomposedQueryPlanId decomposedQueryPlanId,
                                               QueryState queryState,
                                               const std::string& terminationReason) {

    auto lockedSharedQueryCatalogEntryMapping = sharedQueryCatalogEntryMapping.wlock();
    if (!lockedSharedQueryCatalogEntryMapping->contains(sharedQueryId)) {
        NES_ERROR("QueryCatalogService: Query Catalog does not contains the input queryId {}", std::to_string(sharedQueryId));
        throw Exceptions::QueryNotFoundException("Query Catalog does not contains the input queryId "
                                                 + std::to_string(sharedQueryId));
    }

    auto sharedQueryCatalogEntry = (*lockedSharedQueryCatalogEntryMapping)[sharedQueryId];
    auto decomposedQueryPlanMetaData = sharedQueryCatalogEntry->getDecomposedQueryPlanMetaData(decomposedQueryPlanId);

    //Handle new status of the query
    switch (queryState) {
        case QueryState::OPTIMIZING:
        case QueryState::RESTARTING:
        case QueryState::MIGRATING:
        case QueryState::DEPLOYED:
        case QueryState::STOPPED:
        case QueryState::RUNNING:
        case QueryState::MARKED_FOR_SOFT_STOP:
        case QueryState::MARKED_FOR_HARD_STOP:
        case QueryState::MARKED_FOR_MIGRATION:
        case QueryState::MARKED_FOR_FAILURE: {
            decomposedQueryPlanMetaData->updateState(queryState);
            decomposedQueryPlanMetaData->setTerminationReason(terminationReason);
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

void QueryCatalog::checkAndMarkSharedQueryForFailure(SharedQueryId sharedQueryId, DecomposedQueryPlanId decomposedQueryPlanId) {

    NES_INFO("checkAndMarkForFailure sharedQueryId={} subQueryId={}", sharedQueryId, decomposedQueryPlanId);
    //Fetch shared query and query catalogs
    auto [lockedSharedQueryCatalogEntryMapping, lockedQueryCatalogEntryMapping] =
        folly::acquireLocked(sharedQueryCatalogEntryMapping, queryCatalogEntryMapping);
    if (!lockedSharedQueryCatalogEntryMapping->contains(sharedQueryId)) {
        NES_ERROR("Unable to find the shared query plan with id {}", sharedQueryId);
        throw Exceptions::QueryNotFoundException("Unable to find the shared query plan with id " + std::to_string(sharedQueryId));
    }

    auto sharedQueryCatalogEntry = (*lockedSharedQueryCatalogEntryMapping)[sharedQueryId];

    //If query is doing hard stop or has failed or already stopped then soft stop can not be triggered
    auto sharedQueryStatus = sharedQueryCatalogEntry->getQueryState();
    if (sharedQueryStatus == QueryState::MARKED_FOR_FAILURE || sharedQueryStatus == QueryState::FAILED
        || sharedQueryStatus == QueryState::STOPPED) {
        NES_WARNING("QueryCatalogService: Query can not be marked for failure as query in {} status.",
                    magic_enum::enum_name(sharedQueryStatus));
        return;
    }

    auto containedQueryIds = sharedQueryCatalogEntry->getContainedQueryIds();
    for (const auto& containedQueryId : containedQueryIds) {
        if (lockedQueryCatalogEntryMapping->contains(containedQueryId)) {
            auto queryCatalogEntry = (*lockedQueryCatalogEntryMapping)[containedQueryId];
            queryCatalogEntry->setQueryState(QueryState::MARKED_FOR_FAILURE);
        }
    }
    sharedQueryCatalogEntry->setQueryState(QueryState::MARKED_FOR_FAILURE);
    NES_INFO("QueryCatalogService: Shared query id {} is marked as failed", sharedQueryId);
}

bool QueryCatalog::checkAndMarkQueryForHardStop(QueryId queryId) {

    NES_INFO("QueryCatalogService: Handle hard stop request.");
    //lock query catalog and check if query exists
    auto lockedQueryCatalogEntryMapping = queryCatalogEntryMapping.wlock();
    if (!lockedQueryCatalogEntryMapping->contains(queryId)) {
        NES_ERROR("QueryCatalogService: Query Catalog does not contains the input queryId {}", std::to_string(queryId));
        throw Exceptions::QueryNotFoundException("Query Catalog does not contains the input queryId " + std::to_string(queryId));
    }
    auto queryCatalogEntry = (*lockedQueryCatalogEntryMapping)[queryId];
    auto currentStatus = queryCatalogEntry->getQueryState();
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

bool QueryCatalog::updateDecomposedQueryPlanStatus(SharedQueryId sharedQueryId,
                                                   DecomposedQueryPlanId decomposedQueryPlanId,
                                                   QueryState newDecomposedQueryState) {

    switch (newDecomposedQueryState) {
        case QueryState::MARKED_FOR_DEPLOYMENT:
        case QueryState::MARKED_FOR_REDEPLOYMENT:
        case QueryState::MARKED_FOR_MIGRATION: {



            break;
        }
        case QueryState::MARKED_FOR_SOFT_STOP: {
            handleDecomposedQueryPlanMarkedForSoftStop(sharedQueryId, decomposedQueryPlanId);
            break;
        }
        case QueryState::SOFT_STOP_TRIGGERED: {
            handleDecomposedQueryPlanSoftStopTriggered(sharedQueryId, decomposedQueryPlanId);
            break;
        }
        case QueryState::SOFT_STOP_COMPLETED: {
            handleDecomposedQueryPlanSoftStopCompleted(sharedQueryId, decomposedQueryPlanId);
            break;
        }
        default:
            throw Exceptions::InvalidQueryStateException({QueryState::SOFT_STOP_TRIGGERED, QueryState::SOFT_STOP_COMPLETED},
                                                         newDecomposedQueryState);
    }
    return true;
}

bool QueryCatalog::handleDecomposedQueryPlanMarkedForSoftStop(SharedQueryId sharedQueryId,
                                                              DecomposedQueryPlanId decomposedQueryPlanId) {

    NES_INFO("checkAndMarkForSoftStop sharedQueryId={} subQueryId={}", sharedQueryId, decomposedQueryPlanId);
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

    //Mark the decomposed query plan for soft stop
    sharedQueryCatalogEntry->getDecomposedQueryPlanMetaData(decomposedQueryPlanId)->updateState(QueryState::MARKED_FOR_SOFT_STOP);

    //Mark shared query for soft stop and return
    sharedQueryCatalogEntry->setQueryState(QueryState::MARKED_FOR_SOFT_STOP);
    NES_INFO("QueryCatalogService: Shared query id {} is marked as soft stopped", sharedQueryId);
    return true;
}

bool QueryCatalog::handleDecomposedQueryPlanSoftStopCompleted(SharedQueryId sharedQueryId,
                                                              DecomposedQueryPlanId decomposedQueryPlanId) {
    NES_DEBUG("QueryCatalogService: Updating the status of decomposed query with id {} to SOFT_STOP_COMPLETED for shared query "
              "with id {}",
              decomposedQueryPlanId,
              sharedQueryId);

    //Fetch shared query and query catalogs
    auto [lockedSharedQueryCatalogEntryMapping, lockedQueryCatalogEntryMapping] =
        folly::acquireLocked(sharedQueryCatalogEntryMapping, queryCatalogEntryMapping);
    if (!lockedSharedQueryCatalogEntryMapping->contains(sharedQueryId)) {
        NES_ERROR("Unable to find the shared query plan with id {}", sharedQueryId);
        throw Exceptions::QueryNotFoundException("Unable to find the shared query plan with id " + std::to_string(sharedQueryId));
    }

    //Fetch the shared query plan
    auto sharedQueryCatalogEntry = (*lockedSharedQueryCatalogEntryMapping)[sharedQueryId];
    auto currentSharedQueryState = sharedQueryCatalogEntry->getQueryState();

    //todo #4396: when query migration has its own termination type, this function will not be called during migration. remove MIGRATING below
    if (currentSharedQueryState != QueryState::MARKED_FOR_SOFT_STOP && currentSharedQueryState != QueryState::MIGRATING) {
        NES_WARNING("Found shared query with id {} in {} but received SOFT_STOP_COMPLETED for the decomposed query with id {}",
                    sharedQueryId,
                    magic_enum::enum_name(currentSharedQueryState),
                    decomposedQueryPlanId);
        //FIXME: #4396 fix what to do when this occurs
        NES_ASSERT(false,
                   "Found query in " << magic_enum::enum_name(currentSharedQueryState)
                                     << " but received SOFT_STOP_COMPLETED for the decomposed query with id "
                                     << decomposedQueryPlanId << " for shared query id " << sharedQueryId);
    }

    //Get the sub query plan
    auto decomposedQueryPlanMetaData = sharedQueryCatalogEntry->getDecomposedQueryPlanMetaData(decomposedQueryPlanId);

    // check the query sub plan status
    auto currentDecomposedQueryState = decomposedQueryPlanMetaData->getDecomposedQueryPlanStatus();
    if (currentDecomposedQueryState == QueryState::SOFT_STOP_COMPLETED) {
        NES_WARNING("Received multiple soft stop completed for decomposed query with id {} for shared query {}",
                    decomposedQueryPlanId,
                    sharedQueryId);
        NES_WARNING("Skipping remaining operation");
        return false;
    }

    //Update the state of the decomposed query plan
    decomposedQueryPlanMetaData->updateState(newDecomposedQueryPlanState);

    if (currentSharedQueryState == QueryState::MIGRATING) {
        if (newDecomposedQueryPlanState == QueryState::SOFT_STOP_COMPLETED) {
            /* receiving a soft stop for a query sub plan of a migrating query marks the completion of the
                 * migration of that specific query sub plan.*/
            if (newDecomposedQueryPlanState == QueryState::SOFT_STOP_COMPLETED) {
                //todo #4396: once a specifig drain EOS is implemented, remove this as the received state should already equal migration completed
                decomposedQueryPlanMetaData->updateState(QueryState::MIGRATION_COMPLETED);
            }

            /*if the query is in MIGRATING status, check if there are still any querySubPlans that did not finish their
                 * migration. If no subplan in state MIGRATING could be found, the migration of the whole query is complete
                 * and the state is set to RUNNING again */
            bool queryMigrationComplete = true;
            for (auto& decomposedQueryPlanMetaDatum : sharedQueryCatalogEntry->getAllDecomposedQueryPlanMetaData()) {
                auto currentDecomposedQuerySubPlanStatus = decomposedQueryPlanMetaDatum->getDecomposedQueryPlanStatus();
                if (currentDecomposedQuerySubPlanStatus == QueryState::MIGRATING) {
                    queryMigrationComplete = false;
                    break;
                }
                NES_ASSERT(currentDecomposedQuerySubPlanStatus == QueryState::RUNNING
                               || currentDecomposedQuerySubPlanStatus == QueryState::SOFT_STOP_COMPLETED
                               || currentDecomposedQuerySubPlanStatus == QueryState::MIGRATION_COMPLETED
                               || currentDecomposedQuerySubPlanStatus == QueryState::REDEPLOYED,
                           "Unexpected subplan status.");
            }
            if (queryMigrationComplete) {
                sharedQueryCatalogEntry->setQueryState(QueryState::RUNNING);
                for (auto containedQueryId : sharedQueryCatalogEntry->getContainedQueryIds()) {
                    NES_INFO("Query with id {} is now stopped", containedQueryId);
                    auto queryCatalogEntry = (*lockedQueryCatalogEntryMapping)[containedQueryId];
                    queryCatalogEntry->setQueryState(QueryState::RUNNING);
                }
            }
        }
    } else {
        //Check if all decomposed queries are stopped when a decomposed query soft stop completes
        bool stopQuery = true;
        if (newDecomposedQueryPlanState == QueryState::SOFT_STOP_COMPLETED) {
            for (auto& decomposedQueryPlanMetaDatum : sharedQueryCatalogEntry->getAllDecomposedQueryPlanMetaData()) {
                NES_DEBUG("Updating query decomposed query plan status for shared query with id {} and decomposed query plan "
                          "with id {} to {}",
                          sharedQueryId,
                          decomposedQueryPlanMetaDatum->getDecomposedQueryPlanId(),
                          std::string(magic_enum::enum_name(decomposedQueryPlanMetaDatum->getDecomposedQueryPlanStatus())));
                if (decomposedQueryPlanMetaDatum->getDecomposedQueryPlanStatus() != QueryState::SOFT_STOP_COMPLETED) {
                    stopQuery = false;
                    break;
                }
            }
            // Mark the shared query and contained queries as stopped if all decomposed query plans are stopped
            if (stopQuery) {
                NES_INFO("Shared Query Plan with id {} is now stopped.", sharedQueryId);
                sharedQueryCatalogEntry->setQueryState(QueryState::STOPPED);
                for (auto containedQueryId : sharedQueryCatalogEntry->getContainedQueryIds()) {
                    NES_INFO("Query with id {} is now stopped", containedQueryId);
                    auto queryCatalogEntry = (*lockedQueryCatalogEntryMapping)[containedQueryId];
                    queryCatalogEntry->setQueryState(QueryState::STOPPED);
                }
            }
        }
    }
    return true;
}

bool QueryCatalog::handleDecomposedQueryPlanSoftStopTriggered(SharedQueryId sharedQueryId,
                                                              DecomposedQueryPlanId decomposedQueryPlanId) {
    NES_DEBUG("Handling soft stop triggered for decomposed query with id {} for shared query with id {}",
              decomposedQueryPlanId,
              sharedQueryId);

    //Fetch shared query and query catalogs
    auto [lockedSharedQueryCatalogEntryMapping, lockedQueryCatalogEntryMapping] =
        folly::acquireLocked(sharedQueryCatalogEntryMapping, queryCatalogEntryMapping);
    if (!lockedSharedQueryCatalogEntryMapping->contains(sharedQueryId)) {
        NES_ERROR("Unable to find the shared query plan with id {}", sharedQueryId);
        throw Exceptions::QueryNotFoundException("Unable to find the shared query plan with id " + std::to_string(sharedQueryId));
    }

    //Fetch the shared query plan
    auto sharedQueryCatalogEntry = (*lockedSharedQueryCatalogEntryMapping)[sharedQueryId];
    auto currentSharedQueryState = sharedQueryCatalogEntry->getQueryState();

    //Get the decomposed query plan
    auto decomposedQueryPlanMetaData = sharedQueryCatalogEntry->getDecomposedQueryPlanMetaData(decomposedQueryPlanId);
    //check the decomposed query plan status
    auto currentDecomposedQueryState = decomposedQueryPlanMetaData->getDecomposedQueryPlanStatus();

    if (currentDecomposedQueryState == QueryState::SOFT_STOP_COMPLETED) {
        NES_ERROR("Received soft stop triggered for decomposed query with id {} for shared query {} but decomposed query is "
                  "already marked as "
                  "soft stop completed.",
                  decomposedQueryPlanId,
                  sharedQueryId);
        NES_WARNING("Skipping remaining operation");
        return false;
    } else if (currentDecomposedQueryState == QueryState::SOFT_STOP_TRIGGERED) {
        NES_ERROR("Received multiple soft stop triggered for decomposed query with id {} for shared query {}",
                  decomposedQueryPlanId,
                  sharedQueryId);
        NES_WARNING("Skipping remaining operation");
        return false;
    } else {
        //Update the state of the decomposed query plan
        decomposedQueryPlanMetaData->updateState(QueryState::SOFT_STOP_TRIGGERED);
        return true;
    }
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

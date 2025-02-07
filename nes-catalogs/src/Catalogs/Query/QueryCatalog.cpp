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
#include <Catalogs/Util/PlanJsonGenerator.hpp>
#include <Exceptions/InvalidArgumentException.hpp>
#include <Identifiers/NESStrongTypeJson.hpp>
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

QueryState QueryCatalog::getQueryState(QueryId queryId) {
    auto lockedQueryCatalogEntryMapping = queryCatalogEntryMapping.wlock();
    //Check if query exists
    if (!lockedQueryCatalogEntryMapping->contains(queryId)) {
        NES_ERROR("QueryCatalogService: Query Catalog does not contains the input queryId {}", queryId);
        throw Exceptions::QueryNotFoundException(fmt::format("Query Catalog does not contains the input queryId {}", queryId));
    }
    return (*lockedQueryCatalogEntryMapping)[queryId]->getQueryState();
}

void QueryCatalog::linkSharedQuery(QueryId queryId, NES::SharedQueryId sharedQueryId) {
    //Fetch shared query and query catalogs
    auto [lockedSharedQueryCatalogEntryMapping, lockedQueryCatalogEntryMapping] =
        folly::acquireLocked(sharedQueryCatalogEntryMapping, queryCatalogEntryMapping);

    //Check if query exists
    if (!lockedQueryCatalogEntryMapping->contains(queryId)) {
        NES_ERROR("QueryCatalogService: Query Catalog does not contains the input queryId {}", queryId);
        throw Exceptions::QueryNotFoundException(fmt::format("Query Catalog does not contains the input queryId {}", queryId));
    }

    //Check if shared query exists
    if (!lockedSharedQueryCatalogEntryMapping->contains(sharedQueryId)) {
        NES_ERROR("QueryCatalogService: Shared Query Catalog does not contains the input shared queryId {}", sharedQueryId);
        throw Exceptions::QueryNotFoundException(
            fmt::format("Shared Query Catalog does not contains the input shared queryId {}", sharedQueryId));
    }

    auto queryCatalogEntry = (*lockedQueryCatalogEntryMapping)[queryId];
    queryCatalogEntry->setSharedQueryId(sharedQueryId);
    auto sharedQueryCatalogEntry = (*lockedSharedQueryCatalogEntryMapping)[sharedQueryId];
    sharedQueryCatalogEntry->addQueryId(queryId);
}

SharedQueryId QueryCatalog::getLinkedSharedQueryId(QueryId queryId) {
    //Fetch shared query and query catalogs
    auto lockedQueryCatalogEntryMapping = queryCatalogEntryMapping.wlock();
    //Check if query exists
    if (!lockedQueryCatalogEntryMapping->contains(queryId)) {
        NES_ERROR("QueryCatalogService: Query Catalog does not contains the input queryId {}", queryId);
        throw Exceptions::QueryNotFoundException(fmt::format("Query Catalog does not contains the input queryId {}", queryId));
    }

    auto queryCatalogEntry = (*lockedQueryCatalogEntryMapping)[queryId];
    return queryCatalogEntry->getSharedQueryId();
}

void QueryCatalog::updateQueryStatus(QueryId queryId, QueryState queryStatus, const std::string& terminationReason) {

    //Handle new status of the query
    switch (queryStatus) {
        case QueryState::REGISTERED:
        case QueryState::OPTIMIZING:
        case QueryState::RESTARTING:
        case QueryState::MIGRATING:
        case QueryState::DEPLOYED:
        case QueryState::RUNNING:
        case QueryState::EXPLAINED:
        case QueryState::STOPPED:
        case QueryState::FAILED: {
            auto lockedQueryCatalogEntryMapping = queryCatalogEntryMapping.wlock();
            //Check if query exists
            if (!lockedQueryCatalogEntryMapping->contains(queryId)) {
                NES_ERROR("QueryCatalogService: Query Catalog does not contains the input queryId {}", queryId);
                throw Exceptions::QueryNotFoundException(
                    fmt::format("Query Catalog does not contains the input queryId {}", queryId));
            }

            auto queryCatalogEntry = (*lockedQueryCatalogEntryMapping)[queryId];
            queryCatalogEntry->setQueryState(queryStatus);
            queryCatalogEntry->setTerminationReason(terminationReason);
            break;
        }
        case QueryState::MARKED_FOR_HARD_STOP: {
            checkAndMarkQueryForHardStop(queryId);
            break;
        }
        default:
            throw Exceptions::InvalidQueryStateException({QueryState::REGISTERED,
                                                          QueryState::OPTIMIZING,
                                                          QueryState::RESTARTING,
                                                          QueryState::MIGRATING,
                                                          QueryState::DEPLOYED,
                                                          QueryState::RUNNING,
                                                          QueryState::STOPPED,
                                                          QueryState::FAILED,
                                                          QueryState::MARKED_FOR_HARD_STOP},
                                                         queryStatus);
    }
}

bool QueryCatalog::checkAndMarkQueryForHardStop(QueryId queryId) {

    NES_INFO("QueryCatalogService: Handle hard stop request.");
    //lock query catalog and check if query exists
    auto lockedQueryCatalogEntryMapping = queryCatalogEntryMapping.wlock();
    if (!lockedQueryCatalogEntryMapping->contains(queryId)) {
        NES_ERROR("QueryCatalogService: Query Catalog does not contains the input queryId {}", queryId);
        throw Exceptions::QueryNotFoundException(fmt::format("Query Catalog does not contains the input queryId {}", queryId));
    }
    auto queryCatalogEntry = (*lockedQueryCatalogEntryMapping)[queryId];
    auto currentStatus = queryCatalogEntry->getQueryState();
    if (currentStatus == QueryState::MARKED_FOR_SOFT_STOP || currentStatus == QueryState::MARKED_FOR_HARD_STOP
        || currentStatus == QueryState::MARKED_FOR_FAILURE || currentStatus == QueryState::DEPLOYED
        || currentStatus == QueryState::STOPPED || currentStatus == QueryState::FAILED) {
        NES_ERROR("QueryCatalog: Found query status already as {}. Ignoring stop query request.",
                  magic_enum::enum_name(queryCatalogEntry->getQueryState()));
        throw Exceptions::InvalidQueryStateException(
            {QueryState::OPTIMIZING, QueryState::REGISTERED, QueryState::DEPLOYED, QueryState::RUNNING, QueryState::RESTARTING},
            currentStatus);
    }
    NES_DEBUG("QueryCatalog: Changing query status to Mark query for stop.");
    queryCatalogEntry->setQueryState(QueryState::MARKED_FOR_HARD_STOP);
    return true;
}

void QueryCatalog::createSharedQueryCatalogEntry(SharedQueryId sharedQueryId,
                                                 std::set<QueryId> queryIds,
                                                 QueryState queryStatus) {
    NES_INFO("Create shared query catalog entry for the shared query with id {}", sharedQueryId);
    auto sharedQueryCatalogEntry = std::make_shared<SharedQueryCatalogEntry>(sharedQueryId, queryIds, queryStatus);
    auto lockedSharedQueryCatalogEntryMapping = sharedQueryCatalogEntryMapping.wlock();
    (*lockedSharedQueryCatalogEntryMapping)[sharedQueryId] = std::move(sharedQueryCatalogEntry);
}

void QueryCatalog::updateSharedQueryStatus(SharedQueryId sharedQueryId,
                                           QueryState queryState,
                                           const std::string& terminationReason) {

    NES_DEBUG("Updating status of the shared query plan {} to {}", sharedQueryId, magic_enum::enum_name(queryState));
    //Fetch shared query and query catalogs
    auto [lockedSharedQueryCatalogEntryMapping, lockedQueryCatalogEntryMapping] =
        folly::acquireLocked(sharedQueryCatalogEntryMapping, queryCatalogEntryMapping);
    if (!lockedSharedQueryCatalogEntryMapping->contains(sharedQueryId)) {
        NES_ERROR("QueryCatalogService: Query Catalog does not contains the input queryId {}", sharedQueryId);
        throw Exceptions::QueryNotFoundException(
            fmt::format("Query Catalog does not contains the input queryId {}", sharedQueryId));
    }

    auto sharedQueryCatalogEntry = (*lockedSharedQueryCatalogEntryMapping)[sharedQueryId];

    //Handle new status of the query
    switch (queryState) {
        case QueryState::RESTARTING:
        case QueryState::DEPLOYED:
        case QueryState::MIGRATING:
        case QueryState::OPTIMIZING:
        case QueryState::RUNNING: {
            sharedQueryCatalogEntry->setQueryState(queryState);
            auto containedQueryIds = sharedQueryCatalogEntry->getContainedQueryIds();
            for (const auto& containedQueryId : containedQueryIds) {
                if (lockedQueryCatalogEntryMapping->contains(containedQueryId)) {
                    auto queryCatalogEntry = (*lockedQueryCatalogEntryMapping)[containedQueryId];
                    queryCatalogEntry->setQueryState(queryState);
                }
            }
            break;
        }
        case QueryState::STOPPED:
        case QueryState::FAILED: {
            sharedQueryCatalogEntry->setQueryState(queryState);
            sharedQueryCatalogEntry->setTerminationReason(terminationReason);
            for (const auto& decomposedQueryPlanMetaData : sharedQueryCatalogEntry->getAllDecomposedQueryPlanMetaData()) {
                decomposedQueryPlanMetaData->updateState(queryState);
            }
            auto containedQueryIds = sharedQueryCatalogEntry->getContainedQueryIds();
            for (const auto& containedQueryId : containedQueryIds) {
                if (lockedQueryCatalogEntryMapping->contains(containedQueryId)) {
                    auto queryCatalogEntry = (*lockedQueryCatalogEntryMapping)[containedQueryId];
                    queryCatalogEntry->setQueryState(queryState);
                    queryCatalogEntry->setTerminationReason(terminationReason);
                }
            }
            break;
        }
        default:
            throw Exceptions::InvalidQueryStateException({QueryState::RESTARTING,
                                                          QueryState::DEPLOYED,
                                                          QueryState::STOPPED,
                                                          QueryState::RUNNING,
                                                          QueryState::OPTIMIZING,
                                                          QueryState::FAILED},
                                                         queryState);
    }
}

void QueryCatalog::checkAndMarkSharedQueryForFailure(SharedQueryId sharedQueryId,
                                                     DecomposedQueryId decomposedQueryId,
                                                     DecomposedQueryPlanVersion decomposedQueryVersion) {

    NES_INFO("checkAndMarkForFailure sharedQueryId={} subQueryId={}.{}",
             sharedQueryId,
             decomposedQueryId,
             decomposedQueryVersion);
    //Fetch shared query and query catalogs
    auto [lockedSharedQueryCatalogEntryMapping, lockedQueryCatalogEntryMapping] =
        folly::acquireLocked(sharedQueryCatalogEntryMapping, queryCatalogEntryMapping);
    if (!lockedSharedQueryCatalogEntryMapping->contains(sharedQueryId)) {
        NES_ERROR("Unable to find the shared query plan with id {}", sharedQueryId);
        throw Exceptions::QueryNotFoundException(fmt::format("Unable to find the shared query plan with id {}", sharedQueryId));
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

bool QueryCatalog::updateDecomposedQueryPlanStatus(SharedQueryId sharedQueryId,
                                                   DecomposedQueryId decomposedQueryId,
                                                   DecomposedQueryPlanVersion decomposedQueryPlanVersion,
                                                   QueryState newDecomposedQueryState,
                                                   WorkerId workerId) {

    switch (newDecomposedQueryState) {
        case QueryState::MARKED_FOR_DEPLOYMENT: {
            auto lockedSharedQueryCatalogEntryMapping = sharedQueryCatalogEntryMapping.wlock();
            if (!lockedSharedQueryCatalogEntryMapping->contains(sharedQueryId)) {
                NES_ERROR("QueryCatalogService: Query Catalog does not contains the input queryId {}", sharedQueryId);
                throw Exceptions::QueryNotFoundException(
                    fmt::format("Query Catalog does not contains the input queryId {}", sharedQueryId));
            }
            auto sharedQueryCatalogEntry = (*lockedSharedQueryCatalogEntryMapping)[sharedQueryId];
            sharedQueryCatalogEntry->addDecomposedQueryPlanMetaData(decomposedQueryId,
                                                                    decomposedQueryPlanVersion,
                                                                    workerId,
                                                                    newDecomposedQueryState);
            break;
        }
        case QueryState::RUNNING:
        case QueryState::FAILED:
        case QueryState::DEPLOYED:
        case QueryState::MIGRATING:
        case QueryState::STOPPED:
        case QueryState::MARKED_FOR_HARD_STOP:
        case QueryState::MARKED_FOR_FAILURE:
        case QueryState::MARKED_FOR_REDEPLOYMENT:
        case QueryState::MARKED_FOR_MIGRATION: {
            auto lockedSharedQueryCatalogEntryMapping = sharedQueryCatalogEntryMapping.wlock();
            if (!lockedSharedQueryCatalogEntryMapping->contains(sharedQueryId)) {
                NES_ERROR("QueryCatalogService: Query Catalog does not contains the input queryId {}", sharedQueryId);
                throw Exceptions::QueryNotFoundException(
                    fmt::format("Query Catalog does not contains the input queryId {}", sharedQueryId));
            }
            auto sharedQueryCatalogEntry = (*lockedSharedQueryCatalogEntryMapping)[sharedQueryId];
            auto decomposedQueryPlanMetaData = sharedQueryCatalogEntry->getDecomposedQueryPlanMetaData(decomposedQueryId);
            decomposedQueryPlanMetaData->updateState(newDecomposedQueryState);
            break;
        }
        case QueryState::SOFT_STOP_TRIGGERED: {
            return handleDecomposedQueryPlanSoftStopTriggered(sharedQueryId, decomposedQueryId, decomposedQueryPlanVersion);
        }
        case QueryState::MARKED_FOR_SOFT_STOP: {
            return handleDecomposedQueryPlanMarkedForSoftStop(sharedQueryId, decomposedQueryId, decomposedQueryPlanVersion);
        }
        case QueryState::SOFT_STOP_COMPLETED: {
            return handleDecomposedQueryPlanSoftStopCompleted(sharedQueryId, decomposedQueryId, decomposedQueryPlanVersion);
        }
        default:
            throw Exceptions::InvalidQueryStateException({QueryState::SOFT_STOP_TRIGGERED, QueryState::SOFT_STOP_COMPLETED},
                                                         newDecomposedQueryState);
    }
    return true;
}

QueryState QueryCatalog::getDecomposedQueryPlanSatus(SharedQueryId sharedQueryId, DecomposedQueryId decomposedQueryId) {
    //Fetch shared query and query catalogs
    auto lockedSharedQueryCatalogEntryMapping = sharedQueryCatalogEntryMapping.wlock();
    if (!lockedSharedQueryCatalogEntryMapping->contains(sharedQueryId)) {
        NES_ERROR("Unable to find the shared query plan with id {}", sharedQueryId);
        throw Exceptions::QueryNotFoundException(fmt::format("Unable to find the shared query plan with id {}", sharedQueryId));
    }

    //Fetch the shared query plan
    auto sharedQueryCatalogEntry = (*lockedSharedQueryCatalogEntryMapping)[sharedQueryId];
    //Get the decomposed query plan
    auto decomposedQueryPlanMetaData = sharedQueryCatalogEntry->getDecomposedQueryPlanMetaData(decomposedQueryId);
    //check the decomposed query plan status
    return  decomposedQueryPlanMetaData->getDecomposedQueryPlanStatus();
}


bool QueryCatalog::handleDecomposedQueryPlanSoftStopTriggered(SharedQueryId sharedQueryId,
                                                              DecomposedQueryId decomposedQueryId,
                                                              DecomposedQueryPlanVersion decomposedQueryVersion) {
    NES_DEBUG("Handling soft stop triggered for decomposed query with id {}.{} for shared query with id {}",
              decomposedQueryId,
              decomposedQueryVersion,
              sharedQueryId);

    //Fetch shared query and query catalogs
    auto lockedSharedQueryCatalogEntryMapping = sharedQueryCatalogEntryMapping.wlock();
    if (!lockedSharedQueryCatalogEntryMapping->contains(sharedQueryId)) {
        NES_ERROR("Unable to find the shared query plan with id {}", sharedQueryId);
        throw Exceptions::QueryNotFoundException(fmt::format("Unable to find the shared query plan with id {}", sharedQueryId));
    }

    //Fetch the shared query plan
    auto sharedQueryCatalogEntry = (*lockedSharedQueryCatalogEntryMapping)[sharedQueryId];
    //Get the decomposed query plan
    auto decomposedQueryPlanMetaData = sharedQueryCatalogEntry->getDecomposedQueryPlanMetaData(decomposedQueryId);
    //check the decomposed query plan status
    auto currentDecomposedQueryState = decomposedQueryPlanMetaData->getDecomposedQueryPlanStatus();

    if (currentDecomposedQueryState == QueryState::SOFT_STOP_COMPLETED) {
        NES_WARNING("Received soft stop triggered for decomposed query with id {} for shared query {} but decomposed query is "
                    "already marked as soft stop completed.",
                    decomposedQueryId,
                    sharedQueryId);
        NES_WARNING("Skipping remaining operation");
        return false;
    } else if (currentDecomposedQueryState == QueryState::SOFT_STOP_TRIGGERED) {
        NES_WARNING("Received multiple soft stop triggered for decomposed query with id {} for shared query {}.",
                    decomposedQueryId,
                    sharedQueryId);
        NES_WARNING("Skipping remaining operation");
        return false;
    } else if (currentDecomposedQueryState == QueryState::MARKED_FOR_SOFT_STOP) {
        NES_WARNING("Received soft stop triggered for decomposed query with id {} for shared query {} but decomposed query is "
                    "already marked for soft stop. Current decomposed query plan state {}.",
                    decomposedQueryId,
                    sharedQueryId,
                    magic_enum::enum_name(sharedQueryCatalogEntry->getQueryState()));
        NES_WARNING("Skipping remaining operation");
        return false;
    } else {
        //Update the state of the decomposed query plan
        decomposedQueryPlanMetaData->updateState(QueryState::SOFT_STOP_TRIGGERED);
        return true;
    }
}

bool QueryCatalog::handleDecomposedQueryPlanMarkedForSoftStop(SharedQueryId sharedQueryId,
                                                              DecomposedQueryId decomposedQueryId,
                                                              DecomposedQueryPlanVersion decomposedQueryVersion) {

    NES_INFO("checkAndMarkForSoftStop sharedQueryId={} subQueryId={}.{}",
             sharedQueryId,
             decomposedQueryId,
             decomposedQueryVersion);
    //Fetch shared query and query catalogs
    auto [lockedSharedQueryCatalogEntryMapping, lockedQueryCatalogEntryMapping] =
        folly::acquireLocked(sharedQueryCatalogEntryMapping, queryCatalogEntryMapping);
    if (!lockedSharedQueryCatalogEntryMapping->contains(sharedQueryId)) {
        NES_ERROR("QueryCatalogService: Query Catalog does not contains the input queryId {}", sharedQueryId);
        throw Exceptions::QueryNotFoundException(
            fmt::format("Query Catalog does not contains the input queryId {}", sharedQueryId));
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

    //Mark the decomposed query plan for soft stop
    sharedQueryCatalogEntry->getDecomposedQueryPlanMetaData(decomposedQueryId)->updateState(QueryState::MARKED_FOR_SOFT_STOP);

    if (currentSharedQueryState == QueryState::MARKED_FOR_SOFT_STOP) {
        NES_DEBUG("Shared query {} already marked for soft stop.", sharedQueryId);
        return true;
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

bool QueryCatalog::handleDecomposedQueryPlanSoftStopCompleted(SharedQueryId sharedQueryId,
                                                              DecomposedQueryId decomposedQueryId,
                                                              DecomposedQueryPlanVersion decomposedQueryVersion) {
    NES_DEBUG(
        "QueryCatalogService: Updating the status of decomposed query with id {}.{} to SOFT_STOP_COMPLETED for shared query "
        "with id {}",
        decomposedQueryId,
        decomposedQueryVersion,
        sharedQueryId);

    //Fetch shared query and query catalogs
    auto [lockedSharedQueryCatalogEntryMapping, lockedQueryCatalogEntryMapping] =
        folly::acquireLocked(sharedQueryCatalogEntryMapping, queryCatalogEntryMapping);
    if (!lockedSharedQueryCatalogEntryMapping->contains(sharedQueryId)) {
        NES_ERROR("Unable to find the shared query plan with id {}", sharedQueryId);
        throw Exceptions::QueryNotFoundException(fmt::format("Unable to find the shared query plan with id {}", sharedQueryId));
    }

    //Fetch the shared query plan
    auto sharedQueryCatalogEntry = (*lockedSharedQueryCatalogEntryMapping)[sharedQueryId];
    auto currentSharedQueryState = sharedQueryCatalogEntry->getQueryState();

    //todo #4396: when query migration has its own termination type, this function will not be called during migration. remove MIGRATING below
    if (currentSharedQueryState != QueryState::MARKED_FOR_SOFT_STOP && currentSharedQueryState != QueryState::MIGRATING) {
        NES_WARNING("Found shared query with id {} in {} but received SOFT_STOP_COMPLETED for the decomposed query with id {}.{}",
                    sharedQueryId,
                    magic_enum::enum_name(currentSharedQueryState),
                    decomposedQueryId,
                    decomposedQueryVersion);
        //FIXME: #4396 fix what to do when this occurs
        return false;
    }

    //Get the sub query plan
    auto decomposedQueryPlanMetaData = sharedQueryCatalogEntry->getDecomposedQueryPlanMetaData(decomposedQueryId);

    // check the query sub plan status
    auto currentDecomposedQueryState = decomposedQueryPlanMetaData->getDecomposedQueryPlanStatus();
    if (currentDecomposedQueryState == QueryState::SOFT_STOP_COMPLETED) {
        NES_WARNING("Received multiple soft stop completed for decomposed query with id {} for shared query {}",
                    decomposedQueryId,
                    sharedQueryId);
        NES_WARNING("Skipping remaining operation");
        return false;
    }

    //Update the state of the decomposed query plan
    decomposedQueryPlanMetaData->updateState(QueryState::SOFT_STOP_COMPLETED);

    if (currentSharedQueryState == QueryState::MIGRATING) {

        /* receiving a soft stop for a query sub plan of a migrating query marks the completion of the
                 * migration of that specific query sub plan.*/
        //todo #4396: once a specifig drain EOS is implemented, remove this as the received state should already equal migration completed
        decomposedQueryPlanMetaData->updateState(QueryState::MIGRATION_COMPLETED);
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
                       "Unexpected decomposed query plan status.");
        }
        if (queryMigrationComplete) {
            NES_DEBUG("Migration completed. Marking shared query with id {} as running.", sharedQueryId);
            sharedQueryCatalogEntry->setQueryState(QueryState::RUNNING);
            for (auto containedQueryId : sharedQueryCatalogEntry->getContainedQueryIds()) {
                NES_INFO("Query with id {} is now running", containedQueryId);
                auto queryCatalogEntry = (*lockedQueryCatalogEntryMapping)[containedQueryId];
                queryCatalogEntry->setQueryState(QueryState::RUNNING);
            }
        }

    } else {
        //Check if all decomposed queries are stopped when a decomposed query soft stop completes
        bool stopQuery = true;
        for (auto& decomposedQueryPlanMetaDatum : sharedQueryCatalogEntry->getAllDecomposedQueryPlanMetaData()) {
            NES_DEBUG("Updating query decomposed query plan status for shared query with id {} and decomposed query plan "
                      "with id {} to {}",
                      sharedQueryId,
                      decomposedQueryPlanMetaDatum->getDecomposedQueryId(),
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
    return true;
}

void QueryCatalog::addUpdatedQueryPlan(QueryId queryId, std::string step, QueryPlanPtr updatedQueryPlan) {

    //Fetch query catalogs
    auto lockedQueryCatalogEntryMapping = queryCatalogEntryMapping.wlock();
    if (!lockedQueryCatalogEntryMapping->contains(queryId)) {
        NES_ERROR("QueryCatalogService: Query Catalog does not contains the input queryId {}", queryId);
        throw Exceptions::QueryNotFoundException(fmt::format("Query Catalog does not contains the input queryId {}", queryId));
    }

    //Fetch the shared query plan
    auto queryCatalogEntry = (*lockedQueryCatalogEntryMapping)[queryId];
    queryCatalogEntry->addOptimizationPhase(step, updatedQueryPlan);
    if (step == "Executed Query Plan") {
        queryCatalogEntry->setExecutedQueryPlan(updatedQueryPlan);
    }
}

QueryPlanPtr QueryCatalog::getCopyOfExecutedQueryPlan(QueryId queryId) const {
    //Fetch query catalogs
    auto lockedQueryCatalogEntryMapping = queryCatalogEntryMapping.rlock();
    if (!lockedQueryCatalogEntryMapping->contains(queryId)) {
        NES_ERROR("QueryCatalogService: Query Catalog does not contains the input queryId {}", queryId);
        throw Exceptions::QueryNotFoundException(fmt::format("Query Catalog does not contains the input queryId {}", queryId));
    }

    //Fetch the shared query plan
    auto queryCatalogEntry = (*lockedQueryCatalogEntryMapping).at(queryId);
    return queryCatalogEntry->getExecutedQueryPlan()->copy();
}

QueryPlanPtr QueryCatalog::getCopyOfLogicalInputQueryPlan(const QueryId& queryId) const {
    //Fetch query catalogs
    auto lockedQueryCatalogEntryMapping = queryCatalogEntryMapping.rlock();
    if (!lockedQueryCatalogEntryMapping->contains(queryId)) {
        NES_ERROR("QueryCatalogService: Query Catalog does not contains the input queryId {}", queryId);
        throw Exceptions::QueryNotFoundException("Query Catalog does not contains the input queryId " + queryId.toString());
    }

    //Fetch the shared query plan
    auto queryCatalogEntry = (*lockedQueryCatalogEntryMapping).at(queryId);
    return queryCatalogEntry->getInputQueryPlan()->copy();
}

nlohmann::json QueryCatalog::getQueryEntry(QueryId queryId) {

    auto lockedQueryCatalogEntryMapping = queryCatalogEntryMapping.wlock();
    if (!lockedQueryCatalogEntryMapping->contains(queryId)) {
        NES_ERROR("QueryCatalogService: Query Catalog does not contains the input queryId {}", queryId);
        throw Exceptions::QueryNotFoundException(fmt::format("Query Catalog does not contains the input queryId {}", queryId));
    }

    nlohmann::json response;
    auto queryCatalogEntry = (*lockedQueryCatalogEntryMapping)[queryId];
    response["queryId"] = queryId;
    response["queryString"] = queryCatalogEntry->getQueryString();
    response["status"] = magic_enum::enum_name(queryCatalogEntry->getQueryState());
    response["queryPlan"] = queryCatalogEntry->getInputQueryPlan()->toString();
    response["queryMetaData"] = queryCatalogEntry->getMetaInformation();
    response["history"] = queryCatalogEntry->getHistory();
    for (auto const& [phaseName, queryPlan] : queryCatalogEntry->getOptimizationPhases()) {
        auto queryPlanJson = PlanJsonGenerator::getQueryPlanAsJson(queryPlan);
        response[phaseName] = queryPlanJson;
    }
    return response;
}

nlohmann::json QueryCatalog::getQueryEntriesWithStatus(const std::string& queryState) {

    auto state = magic_enum::enum_cast<QueryState>(queryState);
    if (state.has_value()) {
        NES_INFO("QueryCatalog : fetching all queryIdAndCatalogEntryMapping with status {}", queryState);
        auto lockedQueryCatalogEntryMapping = queryCatalogEntryMapping.wlock();
        nlohmann::json response;
        for (auto const& [queryId, queryCatalogEntry] : (*lockedQueryCatalogEntryMapping)) {
            if (queryCatalogEntry->getQueryState() == state) {
                nlohmann::json entry;
                entry["queryId"] = queryId;
                entry["queryString"] = queryCatalogEntry->getQueryString();
                entry["queryStatus"] = magic_enum::enum_name(queryCatalogEntry->getQueryState());
                entry["queryPlan"] = queryCatalogEntry->getInputQueryPlan()->toString();
                entry["queryMetaData"] = queryCatalogEntry->getMetaInformation();
                response.push_back(entry);
            }
        }
        return response;
    } else {
        NES_ERROR("No valid query status to parse");
        throw InvalidArgumentException("QueryState", queryState);
    }
}

nlohmann::json QueryCatalog::getAllQueryEntries() {
    NES_INFO("QueryCatalog : get all queryIdAndCatalogEntryMapping");
    auto lockedQueryCatalogEntryMapping = queryCatalogEntryMapping.wlock();
    nlohmann::json response;
    for (auto const& [queryId, queryCatalogEntry] : (*lockedQueryCatalogEntryMapping)) {
        nlohmann::json entry;
        entry["queryId"] = queryId;
        entry["queryString"] = queryCatalogEntry->getQueryString();
        entry["queryStatus"] = magic_enum::enum_name(queryCatalogEntry->getQueryState());
        entry["queryPlan"] = queryCatalogEntry->getQueryString();
        entry["queryMetaData"] = queryCatalogEntry->getMetaInformation();
        response.push_back(entry);
    }
    return response;
}

void QueryCatalog::reset() {
    NES_TRACE("QueryCatalog: clear query catalog");
    queryCatalogEntryMapping->clear();
    sharedQueryCatalogEntryMapping->clear();
}

}// namespace NES::Catalogs::Query

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
#include <Catalogs/Query/QueryCatalog.hpp>
#include <Catalogs/Query/QueryCatalogEntry.hpp>
#include <Catalogs/Query/QueryCatalogService.hpp>
#include <Catalogs/Query/QuerySubPlanMetaData.hpp>
#include <Exceptions/InvalidArgumentException.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/magicenum/magic_enum.hpp>

namespace NES {
QueryCatalogService::QueryCatalogService(Catalogs::Query::QueryCatalogPtr queryCatalog) : queryCatalog(std::move(queryCatalog)) {}

Catalogs::Query::QueryCatalogEntryPtr
QueryCatalogService::createNewEntry(const std::string& queryString,
                                    const QueryPlanPtr& queryPlan,
                                    const Optimizer::PlacementStrategy placementStrategyName) {
    std::unique_lock lock(serviceMutex);
    return queryCatalog->createNewEntry(queryString, queryPlan, placementStrategyName);
}

bool QueryCatalogService::checkAndMarkForSoftStop(SharedQueryId sharedQueryId,
                                                  DecomposedQueryPlanId subPlanId, OperatorId operatorId) {
    std::unique_lock lock(serviceMutex);

    NES_INFO("checkAndMarkForSoftStop sharedQueryId={} subQueryId={} source={}", sharedQueryId, subPlanId, operatorId);
    //Fetch query catalog entries
    auto queryCatalogEntries = queryCatalog->getQueryCatalogEntriesForSharedQueryId(sharedQueryId);
    for (auto& queryCatalogEntry : queryCatalogEntries) {
        //If query is doing hard stop or has failed or already stopped then soft stop can not be triggered
        auto currentQueryState = queryCatalogEntry->getQueryState();
        if (currentQueryState == QueryState::MARKED_FOR_HARD_STOP || currentQueryState == QueryState::FAILED
            || currentQueryState == QueryState::STOPPED) {
            NES_WARNING("QueryCatalogService: Soft stop can not be initiated as query in {} status.",
                        queryCatalogEntry->getQueryStatusAsString());
            return false;
        }
    }

    //Mark queries for soft stop and return
    for (auto& queryCatalogEntry : queryCatalogEntries) {
        queryCatalogEntry->setQueryStatus(QueryState::MARKED_FOR_SOFT_STOP);
    }
    NES_INFO("QueryCatalogService: Shared query id {} is marked as soft stopped", sharedQueryId);
    return true;
}

bool QueryCatalogService::checkAndMarkForHardStop(QueryId queryId) {
    std::unique_lock lock(serviceMutex);

    NES_INFO("QueryCatalogService: Handle hard stop request.");

    //Check if query exists
    if (!queryCatalog->queryExists(queryId)) {
        NES_ERROR("QueryCatalogService: Query Catalog does not contains the input queryId {}", std::to_string(queryId));
        throw Exceptions::QueryNotFoundException("Query Catalog does not contains the input queryId " + std::to_string(queryId));
    }
    auto queryCatalogEntry = queryCatalog->getQueryCatalogEntry(queryId);

    QueryState currentStatus = queryCatalogEntry->getQueryState();

    if (currentStatus == QueryState::MARKED_FOR_SOFT_STOP || currentStatus == QueryState::MARKED_FOR_HARD_STOP
        || currentStatus == QueryState::MARKED_FOR_FAILURE || currentStatus == QueryState::DEPLOYED
        || currentStatus == QueryState::STOPPED || currentStatus == QueryState::FAILED) {
        NES_ERROR("QueryCatalog: Found query status already as {}. Ignoring stop query request.",
                  queryCatalogEntry->getQueryStatusAsString());
        return false;
    }
    NES_DEBUG("QueryCatalog: Changing query status to Mark query for stop.");
    queryCatalogEntry->setQueryStatus(QueryState::MARKED_FOR_HARD_STOP);
    return true;
}

void QueryCatalogService::checkAndMarkForFailure(SharedQueryId sharedQueryId, DecomposedQueryPlanId querySubPlanId) {
    std::unique_lock lock(serviceMutex);

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
        queryCatalogEntry->setQueryStatus(QueryState::MARKED_FOR_FAILURE);
        for (const auto& subQueryPlanMetaData : queryCatalogEntry->getAllSubQueryPlanMetaData()) {
            //Mark the sub query plan as already failed for which the failure message was received
            if (subQueryPlanMetaData->getQuerySubPlanId() == querySubPlanId) {
                subQueryPlanMetaData->updateStatus(QueryState::FAILED);
            } else {
                subQueryPlanMetaData->updateStatus(QueryState::MARKED_FOR_FAILURE);
            }
        }
    }
    NES_INFO("QueryCatalogService: Shared query id {} is marked as failed", sharedQueryId);
}

Catalogs::Query::QueryCatalogEntryPtr QueryCatalogService::getEntryForQuery(QueryId queryId) {
    std::unique_lock lock(serviceMutex);

    //Check if query exists
    if (!queryCatalog->queryExists(queryId)) {
        NES_ERROR("QueryCatalogService: Query Catalog does not contains the input queryId {}", std::to_string(queryId));
        throw Exceptions::QueryNotFoundException("Query Catalog does not contains the input queryId " + std::to_string(queryId));
    }

    //return query catalog entry
    return queryCatalog->getQueryCatalogEntry(queryId);
}

std::map<uint64_t, std::string> QueryCatalogService::getAllQueriesInStatus(std::string queryStatus) {
    std::unique_lock lock(serviceMutex);

    auto status = magic_enum::enum_cast<QueryState>(queryStatus);
    if (status.has_value()) {
        //return queryIdAndCatalogEntryMapping with status
        return queryCatalog->getQueriesWithStatus(status.value());
    } else {
        NES_ERROR("No valid query status to parse");
        throw InvalidArgumentException("status", queryStatus);
    }
}

std::map<uint64_t, Catalogs::Query::QueryCatalogEntryPtr> QueryCatalogService::getAllEntriesInStatus(std::string queryStatus) {
    std::unique_lock lock(serviceMutex);

    auto status = magic_enum::enum_cast<QueryState>(queryStatus);
    if (status.has_value()) {
        //return queryIdAndCatalogEntryMapping with status
        return queryCatalog->getQueryCatalogEntries(status.value());
    } else {
        NES_ERROR("No valid query status to parse");
        throw InvalidArgumentException("status", queryStatus);
    }
}

bool QueryCatalogService::updateQueryStatus(QueryId queryId, QueryState queryStatus, const std::string& metaInformation) {
    std::unique_lock lock(serviceMutex);

    //Check if query exists
    if (!queryCatalog->queryExists(queryId)) {
        NES_ERROR("QueryCatalogService: Query Catalog does not contains the input queryId {}", std::to_string(queryId));
        throw Exceptions::QueryNotFoundException("Query Catalog does not contains the input queryId " + std::to_string(queryId));
    }

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
            auto queryCatalogEntry = queryCatalog->getQueryCatalogEntry(queryId);
            queryCatalogEntry->setQueryStatus(queryStatus);
            queryCatalogEntry->setMetaInformation(metaInformation);
            for (const auto& subQueryPlanMetaData : queryCatalogEntry->getAllSubQueryPlanMetaData()) {
                subQueryPlanMetaData->updateStatus(queryStatus);
            }
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

void QueryCatalogService::addSubQueryMetaData(QueryId queryId,
                                              DecomposedQueryPlanId querySubPlanId,
                                              uint64_t workerId,
                                              QueryState querySubPlanStatus) {
    std::unique_lock lock(serviceMutex);

    //Check if query exists
    if (!queryCatalog->queryExists(queryId)) {
        NES_ERROR("QueryCatalogService: Query Catalog does not contains the input queryId {}", std::to_string(queryId));
        throw Exceptions::QueryNotFoundException("Query Catalog does not contains the input queryId " + std::to_string(queryId));
    }

    //Fetch the current entry for the query
    auto queryEntry = queryCatalog->getQueryCatalogEntry(queryId);
    //Add new query sub plan
    queryEntry->addQuerySubPlanMetaData(querySubPlanId, workerId, querySubPlanStatus);
}

bool QueryCatalogService::handleSoftStop(SharedQueryId sharedQueryId,
                                         DecomposedQueryPlanId querySubPlanId,
                                         QueryState querySubPlanStatus) {
    std::unique_lock lock(serviceMutex);
    NES_DEBUG("QueryCatalogService: Updating the status of sub query to ({}) for sub query plan with id {} for shared query "
              "plan with id {}",
              std::string(magic_enum::enum_name(querySubPlanStatus)),
              querySubPlanId,
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
                        querySubPlanId,
                        queryId);
            //FIXME: fix what to do when this occurs
            NES_ASSERT(false,
                       "Found query in " << queryCatalogEntry->getQueryStatusAsString() << " but received "
                                         << std::string(magic_enum::enum_name(querySubPlanStatus))
                                         << " for the sub query with id " << querySubPlanId << " for query id " << queryId);
        }

        //Get the sub query plan
        auto querySubPlanMetaData = queryCatalogEntry->getQuerySubPlanMetaData(querySubPlanId);

        // check the query sub plan status
        auto currentStatus = querySubPlanMetaData->getQuerySubPlanStatus();
        if (currentStatus == QueryState::SOFT_STOP_COMPLETED && querySubPlanStatus == QueryState::SOFT_STOP_COMPLETED) {
            NES_WARNING("Received multiple soft stop completed for sub query with id {} for query {}", querySubPlanId, queryId);
            NES_WARNING("Skipping remaining operation");
            continue;
        } else if (currentStatus == QueryState::SOFT_STOP_COMPLETED && querySubPlanStatus == QueryState::SOFT_STOP_TRIGGERED) {
            NES_ERROR("Received soft stop triggered for sub query with id {} for query {} but sub query is already marked as "
                      "soft stop completed.",
                      querySubPlanId,
                      sharedQueryId);
            NES_WARNING("Skipping remaining operation");
            continue;
        } else if (currentStatus == QueryState::SOFT_STOP_TRIGGERED && querySubPlanStatus == QueryState::SOFT_STOP_TRIGGERED) {
            NES_ERROR("Received multiple soft stop triggered for sub query with id {} for query {}",
                      querySubPlanId,
                      sharedQueryId);
            NES_WARNING("Skipping remaining operation");
            continue;
        }

        //todo: refactor this to keep the plans in migrating also when soft stop is triggered
        querySubPlanMetaData->updateStatus(querySubPlanStatus);

        if (currentQueryStatus == QueryState::MIGRATING) {
            if (querySubPlanStatus == QueryState::SOFT_STOP_COMPLETED) {
                /* receiving a soft stop for a query sub plan of a migrating query marks the completion of the
                 * migration of that specific query sub plan.*/
                if (querySubPlanStatus == QueryState::SOFT_STOP_COMPLETED) {
                    //todo #4396: once a specific drain EOS is implemented, remove this as the received state should already equal migration completed
                    querySubPlanMetaData->updateStatus(QueryState::MIGRATION_COMPLETED);
                }

                /*if the query is in MIGRATING status, check if there are still any querySubPlans that did not finish their
                 * migration. If no subplan in state MIGRATING could be found, the migration of the whole query is complete
                 * and the state is set to RUNNING again */
                bool queryMigrationComplete = true;
                for (auto& querySubPlanMetaData : queryCatalogEntry->getAllSubQueryPlanMetaData()) {
                    auto querySubPlanStatus = querySubPlanMetaData->getQuerySubPlanStatus();
                    if (querySubPlanStatus == QueryState::MIGRATING) {
                        queryMigrationComplete = false;
                        break;
                    }
                    NES_ASSERT(querySubPlanStatus == QueryState::RUNNING || querySubPlanStatus == QueryState::SOFT_STOP_COMPLETED
                                   || querySubPlanStatus == QueryState::MIGRATION_COMPLETED
                                   || querySubPlanStatus == QueryState::REDEPLOYED || querySubPlanStatus == QueryState::SOFT_STOP_TRIGGERED,
                               "Unexpected subplan status: " << magic_enum::enum_name(querySubPlanStatus));
                }
                if (queryMigrationComplete) {
                    queryCatalogEntry->setQueryStatus(QueryState::RUNNING);
                }
            }
        } else {
            //Check if all sub queryIdAndCatalogEntryMapping are stopped when a sub query soft stop completes
            bool stopQuery = true;
            if (querySubPlanStatus == QueryState::SOFT_STOP_COMPLETED) {
                for (auto& querySubPlanMetaData : queryCatalogEntry->getAllSubQueryPlanMetaData()) {
                    NES_DEBUG("Updating query subplan status for query id= {} subplan= {} is {}",
                              queryId,
                              querySubPlanMetaData->getQuerySubPlanId(),
                              std::string(magic_enum::enum_name(querySubPlanMetaData->getQuerySubPlanStatus())));
                    if (querySubPlanMetaData->getQuerySubPlanStatus() != QueryState::SOFT_STOP_COMPLETED) {
                        stopQuery = false;
                        break;
                    }
                }
                // Mark the query as stopped if all sub queryIdAndCatalogEntryMapping are stopped
                if (stopQuery) {
                    queryCatalogEntry->setQueryStatus(QueryState::STOPPED);
                    NES_INFO("Query with id {} is now stopped", queryCatalogEntry->getQueryId());
                }
            }
        }
    }
    return true;
}

bool QueryCatalogService::updateQuerySubPlanStatus(SharedQueryId sharedQueryId,
                                                   DecomposedQueryPlanId querySubPlanId,
                                                   QueryState querySubPlanStatus) {
    std::unique_lock lock(serviceMutex);

    switch (querySubPlanStatus) {
        case QueryState::SOFT_STOP_TRIGGERED:
        case QueryState::SOFT_STOP_COMPLETED: handleSoftStop(sharedQueryId, querySubPlanId, querySubPlanStatus); break;
        default:
            throw Exceptions::InvalidQueryStateException({QueryState::SOFT_STOP_TRIGGERED, QueryState::SOFT_STOP_COMPLETED},
                                                         querySubPlanStatus);
    }
    return true;
}

void QueryCatalogService::addUpdatedQueryPlan(QueryId queryId, std::string step, QueryPlanPtr updatedQueryPlan) {
    std::unique_lock lock(serviceMutex);

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

std::map<uint64_t, Catalogs::Query::QueryCatalogEntryPtr> QueryCatalogService::getAllQueryCatalogEntries() {
    std::unique_lock lock(serviceMutex);
    return queryCatalog->getAllQueryCatalogEntries();
}

void QueryCatalogService::clearQueries() {
    std::unique_lock lock(serviceMutex);
    queryCatalog->clearQueries();
}

void QueryCatalogService::resetSubQueryMetaData(QueryId queryId) {
    std::unique_lock lock(serviceMutex);

    //Check if query exists
    if (!queryCatalog->queryExists(queryId)) {
        NES_ERROR("QueryCatalogService: Query Catalog does not contains the input queryId {}", std::to_string(queryId));
        throw Exceptions::QueryNotFoundException("Query Catalog does not contains the input queryId " + std::to_string(queryId));
    }

    auto queryCatalogEntry = queryCatalog->getQueryCatalogEntry(queryId);
    queryCatalogEntry->removeAllQuerySubPlanMetaData();
}

void QueryCatalogService::mapSharedQueryPlanId(SharedQueryId sharedQueryId, QueryId queryId) {
    std::unique_lock lock(serviceMutex);
    //Fetch the catalog entry
    auto catalogEntry = queryCatalog->getQueryCatalogEntry(queryId);
    queryCatalog->mapSharedQueryPlanId(sharedQueryId, catalogEntry);
}

void QueryCatalogService::removeSharedQueryPlanMapping(SharedQueryId sharedQueryId) {
    std::unique_lock lock(serviceMutex);
    queryCatalog->removeSharedQueryPlanIdMappings(sharedQueryId);
}

std::vector<QueryId> QueryCatalogService::getQueryIdsForSharedQueryId(SharedQueryId sharedQueryId) {
    std::unique_lock lock(serviceMutex);

    std::vector<QueryId> queryIds;
    //Fetch query catalog entries
    auto queryCatalogEntries = queryCatalog->getQueryCatalogEntriesForSharedQueryId(sharedQueryId);
    //create collection of query ids
    for (auto& queryCatalogEntry : queryCatalogEntries) {
        queryIds.emplace_back(queryCatalogEntry->getQueryId());
    }
    return queryIds;
}
}// namespace NES

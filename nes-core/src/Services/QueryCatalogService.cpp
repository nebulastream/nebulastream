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

#include <Catalogs/Query/QueryCatalog.hpp>
#include <Catalogs/Query/QueryCatalogEntry.hpp>
#include <Catalogs/Query/QuerySubPlanMetaData.hpp>
#include <Exceptions/InvalidQueryStatusException.hpp>
#include <Exceptions/QueryNotFoundException.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Services/QueryCatalogService.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES {

QueryCatalogService::QueryCatalogService(Catalogs::Query::QueryCatalogPtr queryCatalog) : queryCatalog(std::move(queryCatalog)) {}

Catalogs::Query::QueryCatalogEntryPtr QueryCatalogService::createNewEntry(const std::string& queryString,
                                                                          const QueryPlanPtr& queryPlan,
                                                                          const std::string& placementStrategyName) {
    std::unique_lock lock(serviceMutex);
    return queryCatalog->createNewEntry(queryString, queryPlan, placementStrategyName);
}

bool QueryCatalogService::checkAndMarkForSoftStop(SharedQueryId sharedQueryId, QuerySubPlanId subPlanId, OperatorId operatorId) {
    std::unique_lock lock(serviceMutex);

    NES_INFO("checkAndMarkForSoftStop sharedQueryId=" << sharedQueryId << " subQueryId=" << subPlanId
                                                      << " source=" << operatorId);
    //Fetch query catalog entries
    auto queryCatalogEntries = queryCatalog->getQueryCatalogEntriesForSharedQueryId(sharedQueryId);
    for (auto& queryCatalogEntry : queryCatalogEntries) {
        //If query is doing hard stop or has failed or already stopped then soft stop can not be triggered
        QueryStatus::Value currentQueryStatus = queryCatalogEntry->getQueryStatus();
        if (currentQueryStatus == QueryStatus::MarkedForHardStop || currentQueryStatus == QueryStatus::Failed
            || currentQueryStatus == QueryStatus::Stopped) {
            NES_WARNING("QueryCatalogService: Soft stop can not be initiated as query in "
                        << queryCatalogEntry->getQueryStatusAsString() << " status.");
            return false;
        }
    }

    //Mark queries for soft stop and return
    for (auto& queryCatalogEntry : queryCatalogEntries) {
        queryCatalogEntry->setQueryStatus(QueryStatus::MarkedForSoftStop);
    }
    NES_INFO("QueryCatalogService: Shared query id " << sharedQueryId << " is marked as soft stopped");
    return true;
}

bool QueryCatalogService::checkAndMarkForHardStop(QueryId queryId) {
    std::unique_lock lock(serviceMutex);

    NES_INFO("QueryCatalogService: Handle hard stop request.");

    //Check if query exists
    if (!queryCatalog->queryExists(queryId)) {
        NES_ERROR("QueryCatalogService: Query Catalog does not contains the input queryId " + std::to_string(queryId));
        throw QueryNotFoundException("Query Catalog does not contains the input queryId " + std::to_string(queryId));
    }
    auto queryCatalogEntry = queryCatalog->getQueryCatalogEntry(queryId);

    QueryStatus::Value currentStatus = queryCatalogEntry->getQueryStatus();
    //    if (currentStatus == QueryStatus::Stopped) {
    //        NES_DEBUG("Already stopped!");
    //        return true;
    //    }

    if (currentStatus == QueryStatus::MarkedForSoftStop || currentStatus == QueryStatus::MarkedForHardStop
        || currentStatus == QueryStatus::MarkedForFailure || currentStatus == QueryStatus::Deployed
        || currentStatus == QueryStatus::Stopped || currentStatus == QueryStatus::Failed) {
        NES_ERROR("QueryCatalog: Found query status already as " + queryCatalogEntry->getQueryStatusAsString()
                  + ". Ignoring stop query request.");
        //        throw InvalidQueryStatusException(
        //            {QueryStatus::Optimizing, QueryStatus::Registered, QueryStatus::Deployed, QueryStatus::Running},
        //            currentStatus);
        return false;
    }
    NES_DEBUG("QueryCatalog: Changing query status to Mark query for stop.");
    queryCatalogEntry->setQueryStatus(QueryStatus::MarkedForHardStop);
    return true;
}

bool QueryCatalogService::checkAndMarkForFailure(SharedQueryId sharedQueryId, QuerySubPlanId querySubPlanId) {
    std::unique_lock lock(serviceMutex);

    NES_INFO("checkAndMarkForFailure sharedQueryId=" << sharedQueryId << " subQueryId=" << querySubPlanId);
    //Fetch query catalog entries
    auto queryCatalogEntries = queryCatalog->getQueryCatalogEntriesForSharedQueryId(sharedQueryId);

    if (queryCatalogEntries.empty()) {
        NES_FATAL_ERROR("Unable to find the shared query plan with id " << sharedQueryId);
        throw QueryNotFoundException("Unable to find the shared query plan with id " + std::to_string(sharedQueryId));
    }

    // First perform a check if query can be marked for stop
    for (auto& queryCatalogEntry : queryCatalogEntries) {
        //If query is doing hard stop or has failed or already stopped then soft stop can not be triggered
        QueryStatus::Value currentQueryStatus = queryCatalogEntry->getQueryStatus();
        if (currentQueryStatus == QueryStatus::MarkedForFailure || currentQueryStatus == QueryStatus::Failed
            || currentQueryStatus == QueryStatus::Stopped) {
            NES_WARNING("QueryCatalogService: Query can not be marked for failure as query in "
                        << queryCatalogEntry->getQueryStatusAsString() << " status.");
            return false;
        }
    }

    //Mark queries for failure and return
    for (auto& queryCatalogEntry : queryCatalogEntries) {
        queryCatalogEntry->setQueryStatus(QueryStatus::MarkedForFailure);
        for (const auto& subQueryPlanMetaData : queryCatalogEntry->getAllSubQueryPlanMetaData()) {
            //Mark the sub query plan as already failed for which the failure message was received
            if (subQueryPlanMetaData->getQuerySubPlanId() == querySubPlanId) {
                subQueryPlanMetaData->updateStatus(QueryStatus::Failed);
            } else {
                subQueryPlanMetaData->updateStatus(QueryStatus::MarkedForFailure);
            }
        }
    }
    NES_INFO("QueryCatalogService: Shared query id " << sharedQueryId << " is marked as failed");
    return true;
}

Catalogs::Query::QueryCatalogEntryPtr QueryCatalogService::getEntryForQuery(QueryId queryId) {
    std::unique_lock lock(serviceMutex);

    //Check if query exists
    if (!queryCatalog->queryExists(queryId)) {
        NES_ERROR("QueryCatalogService: Query Catalog does not contains the input queryId " + std::to_string(queryId));
        throw QueryNotFoundException("Query Catalog does not contains the input queryId " + std::to_string(queryId));
    }

    //return query catalog entry
    return queryCatalog->getQueryCatalogEntry(queryId);
}

std::map<uint64_t, std::string> QueryCatalogService::getAllQueriesInStatus(std::string queryStatus) {
    std::unique_lock lock(serviceMutex);

    QueryStatus::Value status = QueryStatus::getFromString(queryStatus);
    //return queryIdAndCatalogEntryMapping with status
    return queryCatalog->getQueriesWithStatus(status);
}

std::map<uint64_t, Catalogs::Query::QueryCatalogEntryPtr> QueryCatalogService::getAllEntriesInStatus(std::string queryStatus) {
    std::unique_lock lock(serviceMutex);

    QueryStatus::Value status = QueryStatus::getFromString(queryStatus);
    //return queryIdAndCatalogEntryMapping with status
    return queryCatalog->getQueryCatalogEntries(status);
}

bool QueryCatalogService::updateQueryStatus(QueryId queryId, QueryStatus::Value queryStatus, const std::string& metaInformation) {
    std::unique_lock lock(serviceMutex);

    //Check if query exists
    if (!queryCatalog->queryExists(queryId)) {
        NES_ERROR("QueryCatalogService: Query Catalog does not contains the input queryId " + std::to_string(queryId));
        throw QueryNotFoundException("Query Catalog does not contains the input queryId " + std::to_string(queryId));
    }

    //Handle new status of the query
    switch (queryStatus) {
        case QueryStatus::Registered:
        case QueryStatus::Optimizing:
        case QueryStatus::Restarting:
        case QueryStatus::Migrating:
        case QueryStatus::Deployed:
        case QueryStatus::Stopped:
        case QueryStatus::Running:
        case QueryStatus::Failed: {
            auto queryCatalogEntry = queryCatalog->getQueryCatalogEntry(queryId);
            queryCatalogEntry->setQueryStatus(queryStatus);
            queryCatalogEntry->setMetaInformation(metaInformation);
            for (const auto& subQueryPlanMetaData : queryCatalogEntry->getAllSubQueryPlanMetaData()) {
                subQueryPlanMetaData->updateStatus(queryStatus);
            }
            return true;
        }
        default:
            throw InvalidQueryStatusException({QueryStatus::Registered,
                                               QueryStatus::Optimizing,
                                               QueryStatus::Restarting,
                                               QueryStatus::Migrating,
                                               QueryStatus::Stopped,
                                               QueryStatus::Running,
                                               QueryStatus::Failed},
                                              queryStatus);
    }
}

void QueryCatalogService::addSubQueryMetaData(QueryId queryId, QuerySubPlanId querySubPlanId, uint64_t workerId) {
    std::unique_lock lock(serviceMutex);

    //Check if query exists
    if (!queryCatalog->queryExists(queryId)) {
        NES_ERROR("QueryCatalogService: Query Catalog does not contains the input queryId " + std::to_string(queryId));
        throw QueryNotFoundException("Query Catalog does not contains the input queryId " + std::to_string(queryId));
    }

    //Fetch the current entry for the query
    auto queryEntry = queryCatalog->getQueryCatalogEntry(queryId);
    //Add new query sub plan
    queryEntry->addQuerySubPlanMetaData(querySubPlanId, workerId);
}

bool QueryCatalogService::handleSoftStop(SharedQueryId sharedQueryId,
                                         QuerySubPlanId querySubPlanId,
                                         QueryStatus::Value subQueryStatus) {
    std::unique_lock lock(serviceMutex);
    NES_DEBUG("QueryCatalogService: Updating the status of sub query to (" << QueryStatus::toString(subQueryStatus)
                                                                           << ") for sub query plan with id " << querySubPlanId
                                                                           << " for shared query plan with id " << sharedQueryId);

    //Fetch query catalog entries
    auto queryCatalogEntries = queryCatalog->getQueryCatalogEntriesForSharedQueryId(sharedQueryId);
    for (auto& queryCatalogEntry : queryCatalogEntries) {
        auto queryId = queryCatalogEntry->getQueryId();
        //Check if query is in correct status
        auto currentQueryStatus = queryCatalogEntry->getQueryStatus();
        if (currentQueryStatus != QueryStatus::MarkedForSoftStop) {
            NES_WARNING("Found query in " << queryCatalogEntry->getQueryStatusAsString() << " but received "
                                          << QueryStatus::toString(subQueryStatus) << " for the sub query with id "
                                          << querySubPlanId << " for query id " << queryId);
            //FIXME: fix what to do when this occurs
            NES_ASSERT(false,
                       "Found query in " << queryCatalogEntry->getQueryStatusAsString() << " but received "
                                         << QueryStatus::toString(subQueryStatus) << " for the sub query with id "
                                         << querySubPlanId << " for query id " << queryId);
        }

        //Get the sub query plan
        auto querySubPlanMetaData = queryCatalogEntry->getQuerySubPlanMetaData(querySubPlanId);

        // check the query sub plan status
        auto currentStatus = querySubPlanMetaData->getQuerySubPlanStatus();
        if (currentStatus == QueryStatus::SoftStopCompleted && subQueryStatus == QueryStatus::SoftStopCompleted) {
            NES_WARNING("Received multiple soft stop completed for sub query with id " << querySubPlanId << " for query "
                                                                                       << queryId);
            NES_WARNING("Skipping remaining operation");
            continue;
        } else if (currentStatus == QueryStatus::SoftStopCompleted && subQueryStatus == QueryStatus::SoftStopTriggered) {
            NES_ERROR("Received soft stop triggered for sub query with id "
                      << querySubPlanId << " for query " << sharedQueryId
                      << " but sub query is already marked as soft stop completed.");
            NES_WARNING("Skipping remaining operation");
            continue;
        } else if (currentStatus == QueryStatus::SoftStopTriggered && subQueryStatus == QueryStatus::SoftStopTriggered) {
            NES_ERROR("Received multiple soft stop triggered for sub query with id " << querySubPlanId << " for query "
                                                                                     << sharedQueryId);
            NES_WARNING("Skipping remaining operation");
            continue;
        }

        querySubPlanMetaData->updateStatus(subQueryStatus);

        //Check if all sub queryIdAndCatalogEntryMapping are stopped when a sub query soft stop completes
        bool stopQuery = true;
        if (subQueryStatus == QueryStatus::SoftStopCompleted) {
            for (auto& querySubPlanMetaData : queryCatalogEntry->getAllSubQueryPlanMetaData()) {
                NES_DEBUG("Updating query subplan status for query id="
                          << queryId << " subplan=" << querySubPlanMetaData->getQuerySubPlanId() << " is "
                          << QueryStatus::toString(querySubPlanMetaData->getQuerySubPlanStatus()))
                if (querySubPlanMetaData->getQuerySubPlanStatus() != QueryStatus::SoftStopCompleted) {
                    stopQuery = false;
                    break;
                }
            }
            // Mark the query as stopped if all sub queryIdAndCatalogEntryMapping are stopped
            if (stopQuery) {
                queryCatalogEntry->setQueryStatus(QueryStatus::Stopped);
                NES_INFO("Query with id " << queryCatalogEntry->getQueryId() << " is now stopped");
            }
        }
    }
    return true;
}

bool QueryCatalogService::updateQuerySubPlanStatus(SharedQueryId sharedQueryId,
                                                   QuerySubPlanId querySubPlanId,
                                                   QueryStatus::Value subQueryStatus) {
    std::unique_lock lock(serviceMutex);

    switch (subQueryStatus) {
        case QueryStatus::SoftStopTriggered:
        case QueryStatus::SoftStopCompleted: handleSoftStop(sharedQueryId, querySubPlanId, subQueryStatus); break;
        default:
            throw InvalidQueryStatusException({QueryStatus::SoftStopTriggered, QueryStatus::SoftStopCompleted}, subQueryStatus);
    }
    return true;
}

void QueryCatalogService::addUpdatedQueryPlan(QueryId queryId, std::string step, QueryPlanPtr updatedQueryPlan) {
    std::unique_lock lock(serviceMutex);

    //Check if query exists
    if (!queryCatalog->queryExists(queryId)) {
        NES_ERROR("QueryCatalogService: Query Catalog does not contains the input queryId " + std::to_string(queryId));
        throw QueryNotFoundException("Query Catalog does not contains the input queryId " + std::to_string(queryId));
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
        NES_ERROR("QueryCatalogService: Query Catalog does not contains the input queryId " + std::to_string(queryId));
        throw QueryNotFoundException("Query Catalog does not contains the input queryId " + std::to_string(queryId));
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

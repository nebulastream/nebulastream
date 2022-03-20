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
#include <Exceptions/InvalidQueryException.hpp>
#include <Exceptions/InvalidQueryStatusException.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Services/QueryCatalogService.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES {

QueryCatalogService::QueryCatalogService(QueryCatalogPtr queryCatalog) : queryCatalog(std::move(queryCatalog)) {}

QueryCatalogEntryPtr QueryCatalogService::createNewEntry(const std::string& queryString,
                                                         const QueryPlanPtr& queryPlan,
                                                         const std::string& placementStrategyName) {
    std::unique_lock lock(serviceMutex);
    return queryCatalog->createNewEntry(queryString, queryPlan, placementStrategyName);
}

bool QueryCatalogService::checkAndMarkForSoftStop(QueryId queryId) {
    std::unique_lock lock(serviceMutex);

    //Check if query exists
    if (!queryCatalog->queryExists(queryId)) {
        throw InvalidQueryException("Query Catalog does not contains the input queryId " + std::to_string(queryId));
    }

    //Fetch the current entry for the query
    auto queryCatalogEntry = queryCatalog->getQueryCatalogEntry(queryId);
    QueryStatus::Value currentQueryStatus = queryCatalogEntry->getQueryStatus();

    //If query is doing hard stop or has failed or already stopped then soft stop can not be triggered
    if (currentQueryStatus == QueryStatus::MarkedForHardStop || currentQueryStatus == QueryStatus::Failed
        || currentQueryStatus == QueryStatus::Stopped) {
        NES_WARNING("QueryCatalogService: Soft stop can not be initiated as query in "
                    << queryCatalogEntry->getQueryStatusAsString() << " status.");
        return false;
    }

    //Mark query for soft stop and return
    queryCatalogEntry->setQueryStatus(QueryStatus::MarkedForSoftStop);
    return true;
}

bool QueryCatalogService::checkAndMarkForHardStop(QueryId queryId) {
    std::unique_lock lock(serviceMutex);

    NES_INFO("QueryCatalogService: Handle hard stop request.");

    //Check if query exists
    if (!queryCatalog->queryExists(queryId)) {
        throw InvalidQueryException("Query Catalog does not contains the input queryId " + std::to_string(queryId));
    }
    auto queryCatalogEntry = queryCatalog->getQueryCatalogEntry(queryId);

    QueryStatus::Value currentStatus = queryCatalogEntry->getQueryStatus();
    if (currentStatus == QueryStatus::MarkedForSoftStop || currentStatus == QueryStatus::MarkedForHardStop
        || currentStatus == QueryStatus::Stopped || currentStatus == QueryStatus::Failed) {
        NES_ERROR("QueryCatalog: Found query status already as " + queryCatalogEntry->getQueryStatusAsString()
                  + ". Ignoring stop query request.");
        throw InvalidQueryStatusException({QueryStatus::Scheduling, QueryStatus::Registered, QueryStatus::Running},
                                          currentStatus);
    }
    NES_INFO("QueryCatalog: Changing query status to Mark query for stop.");
    queryCatalogEntry->setQueryStatus(QueryStatus::MarkedForHardStop);
    return true;
}

QueryCatalogEntryPtr QueryCatalogService::getEntryForQuery(QueryId queryId) {
    std::unique_lock lock(serviceMutex);

    //Check if query exists
    if (!queryCatalog->queryExists(queryId)) {
        throw InvalidQueryException("Query Catalog does not contains the input queryId " + std::to_string(queryId));
    }

    //return query catalog entry
    return queryCatalog->getQueryCatalogEntry(queryId);
}

std::map<uint64_t, std::string> QueryCatalogService::getAllEntriesInStatus(std::string queryStatus) {
    std::unique_lock lock(serviceMutex);

    QueryStatus::Value status = QueryStatus::getFromString(queryStatus);
    //return queries with status
    return queryCatalog->getQueriesWithStatus(status);
}

bool QueryCatalogService::updateQueryStatus(QueryId queryId, QueryStatus::Value queryStatus, const std::string& metaInformation) {

    //Check if query exists
    if (!queryCatalog->queryExists(queryId)) {
        throw InvalidQueryException("Query Catalog does not contains the input queryId " + std::to_string(queryId));
    }

    //Handle new status of the query
    switch (queryStatus) {
        case QueryStatus::Registered:
        case QueryStatus::Scheduling:
        case QueryStatus::Restarting:
        case QueryStatus::Migrating:
        case QueryStatus::Stopped:
        case QueryStatus::Running:
        case QueryStatus::Failed: {
            auto queryCatalogEntry = queryCatalog->getQueryCatalogEntry(queryId);
            auto currentStatus = queryCatalogEntry->getQueryStatus();
            if ((currentStatus == QueryStatus::MarkedForHardStop || currentStatus == QueryStatus::Stopped)
                && queryStatus == QueryStatus::Running) {
                NES_ERROR("QueryCatalog: Found query status already as " + queryCatalogEntry->getQueryStatusAsString()
                          + ". Can not set new status as running.");
                throw InvalidQueryStatusException({QueryStatus::MarkedForHardStop, QueryStatus::Stopped}, queryStatus);
            }
            queryCatalogEntry->setQueryStatus(queryStatus);
            queryCatalogEntry->setMetaInformation(metaInformation);
            break;
        }
        default:
            throw InvalidQueryStatusException({QueryStatus::Registered,
                                               QueryStatus::Scheduling,
                                               QueryStatus::Restarting,
                                               QueryStatus::Migrating,
                                               QueryStatus::Stopped,
                                               QueryStatus::Running,
                                               QueryStatus::Failed},
                                              queryStatus);
    }
    return false;
}

void QueryCatalogService::addSubQueryMetaData(QueryId queryId, QuerySubPlanId querySubPlanId, uint64_t workerId) {
    std::unique_lock lock(serviceMutex);

    //Check if query exists
    if (!queryCatalog->queryExists(queryId)) {
        throw InvalidQueryException("Query Catalog does not contains the input queryId " + std::to_string(queryId));
    }

    //Fetch the current entry for the query
    auto queryEntry = queryCatalog->getQueryCatalogEntry(queryId);
    //Add new query sub plan
    queryEntry->addQuerySubPlanMetaData(querySubPlanId, workerId);
}

bool QueryCatalogService::handleSoftStopCompletion(QueryId queryId,
                                                   QuerySubPlanId querySubPlanId,
                                                   QueryStatus::Value subQueryStatus) {

    std::unique_lock lock(serviceMutex);

    NES_INFO("QueryCatalogService: Updating the status of sub query with id " << querySubPlanId << " for query with id "
                                                                              << queryId);

    //Check if query exists
    if (!queryCatalog->queryExists(queryId)) {
        throw InvalidQueryException("Query Catalog does not contains the input queryId " + std::to_string(queryId));
    }

    //Get the query catalog entry
    auto queryCatalogEntry = queryCatalog->getQueryCatalogEntry(queryId);

    //Check if query is in correct status
    auto currentQueryStatus = queryCatalogEntry->getQueryStatus();
    if (currentQueryStatus != QueryStatus::MarkedForSoftStop) {
        NES_ERROR("Found query in " << queryCatalogEntry->getQueryStatusAsString()
                                    << " but received soft stop for the sub query with id " << querySubPlanId
                                    << " for query with id " << queryId);
        return false;
    }

    //Get the sub query plan
    auto querySubPlanMetaData = queryCatalogEntry->getQuerySubPlanMetaData(querySubPlanId);

    // check the query sub plan status
    auto currentStatus = querySubPlanMetaData->getQuerySubPlanStatus();
    if (currentStatus == QueryStatus::SoftStopCompleted && subQueryStatus == QueryStatus::SoftStopCompleted) {
        NES_WARNING("Received multiple soft stop completed for sub query with id " << querySubPlanId << " for query " << queryId);
        NES_WARNING("Skipping remaining operation");
        return true;
    } else if (currentStatus == QueryStatus::SoftStopCompleted && subQueryStatus == QueryStatus::SoftStopTriggered) {
        NES_ERROR("Received soft stop triggered for sub query with id "
                  << querySubPlanId << " for query " << queryId << " but sub query is already marked as soft stop completed.");
        NES_WARNING("Skipping remaining operation");
        return false;
    } else if (currentStatus == QueryStatus::SoftStopTriggered && subQueryStatus == QueryStatus::SoftStopTriggered) {
        NES_ERROR("Received multiple soft stop triggered for sub query with id " << querySubPlanId << " for query " << queryId);
        NES_WARNING("Skipping remaining operation");
        return false;
    }

    querySubPlanMetaData->updateStatus(subQueryStatus);

    //Check if all sub queries are stopped when a sub query soft stop completes
    bool stopQuery = false;
    if (subQueryStatus == QueryStatus::SoftStopCompleted) {
        for (auto& querySubPlanMetaData : queryCatalogEntry->getAllSubQueryPlanMetaData()) {
            if (querySubPlanMetaData->getQuerySubPlanStatus() != QueryStatus::SoftStopCompleted) {
                break;
            }
        }
        stopQuery = true;
    }

    // Mark the query as stopped if all sub queries are stopped
    if (stopQuery) {
        queryCatalogEntry->setQueryStatus(QueryStatus::Stopped);
    }
    return true;
}

bool QueryCatalogService::registerSoftStopTriggered(QueryId queryId, QuerySubPlanId querySubPlanId, bool triggered) {

    if (triggered) {
        updateQuerySubPlanStatus(queryId, querySubPlanId, QueryStatus::SoftStopTriggered);
    }

    //TODO: Ventura how to handle if soft stop didn't triggered?
    return true;
}

bool QueryCatalogService::registerSoftStopCompleted(QueryId queryId, QuerySubPlanId querySubPlanId, bool completed) {

    if (completed) {
        updateQuerySubPlanStatus(queryId, querySubPlanId, QueryStatus::SoftStopTriggered);
    }

    //TODO: Ventura how to handle if soft stop didn't completed?
    return true;
}

void QueryCatalogService::updateQuerySubPlanStatus(QueryId queryId,
                                                   QuerySubPlanId querySubPlanId,
                                                   QueryStatus::Value subQueryStatus) {
    std::unique_lock lock(serviceMutex);

    switch (subQueryStatus) {
        case QueryStatus::SoftStopTriggered:
        case QueryStatus::SoftStopCompleted: handleSoftStopCompletion(queryId, querySubPlanId, subQueryStatus); break;
        default:
            throw InvalidQueryStatusException({QueryStatus::SoftStopTriggered, QueryStatus::SoftStopCompleted}, subQueryStatus);
    }
}

void QueryCatalogService::addUpdatedQueryPlan(QueryId queryId, std::string step, QueryPlanPtr updatedQueryPlan) {
    std::unique_lock lock(serviceMutex);

    //Check if query exists
    if (!queryCatalog->queryExists(queryId)) {
        throw InvalidQueryException("Query Catalog does not contains the input queryId " + std::to_string(queryId));
    }

    auto queryCatalogEntry = queryCatalog->getQueryCatalogEntry(queryId);
    queryCatalogEntry->addOptimizationPhase(step, updatedQueryPlan);

    if (step == "Executed Query Plan") {
        queryCatalogEntry->setExecutedQueryPlan(updatedQueryPlan);
    }
}

std::map<uint64_t, QueryCatalogEntryPtr> QueryCatalogService::getAllQueryCatalogEntries() {
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
        throw InvalidQueryException("Query Catalog does not contains the input queryId " + std::to_string(queryId));
    }

    auto queryCatalogEntry = queryCatalog->getQueryCatalogEntry(queryId);
    queryCatalogEntry->removeAllQuerySubPlanMetaData();
}

}// namespace NES

#include <Catalogs/QueryCatalog.hpp>
#include <Exceptions/ExecutionPlanRollbackException.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <string>

namespace NES {

QueryCatalog::QueryCatalog(TopologyManagerPtr topologyManager, StreamCatalogPtr streamCatalog, GlobalExecutionPlanPtr globalExecutionPlan)
    : topologyManager(topologyManager), streamCatalog(streamCatalog), globalExecutionPlan(globalExecutionPlan), insertDeleteQuery() {
    NES_DEBUG("QueryCatalog()");
}

std::map<std::string, std::string> QueryCatalog::getQueriesWithStatus(std::string status) {

    NES_INFO("QueryCatalog : fetching all queries with status " << status);

    std::transform(status.begin(), status.end(), status.begin(), ::toupper);

    if (stringToQueryStatusMap.find(status) == stringToQueryStatusMap.end()) {
        throw std::invalid_argument("Unknown query status " + status);
    }

    QueryStatus queryStatus = stringToQueryStatusMap[status];
    std::map<std::string, QueryCatalogEntryPtr> queries = getQueries(queryStatus);

    std::map<std::string, std::string> result;
    for (auto [key, value] : queries) {
        result[key] = value->getQueryString();
    }

    NES_INFO("QueryCatalog : found " << result.size() << " all queries with status " << status);
    return result;
}

std::vector<QueryCatalogEntryPtr> QueryCatalog::getQueriesToSchedule() {
    std::unique_lock<std::mutex> lock(insertDeleteQuery);
    NES_INFO("QueryCatalog: Fetching Queries to Schedule");
    std::vector<QueryCatalogEntryPtr> queriesToSchedule;
    if (!schedulingQueue.empty()) {
        int64_t currentBatchSize = 1;
        int64_t totalQueriesToSchedule = schedulingQueue.size();
        //Prepare a batch of queries to schedule
        while (currentBatchSize <= batchSize || currentBatchSize == totalQueriesToSchedule) {
            queriesToSchedule.push_back(schedulingQueue.front());
            schedulingQueue.pop();
            currentBatchSize++;
        }
        NES_INFO("QueryCatalog: Scheduling " << queriesToSchedule.size() << " queries.");
        return queriesToSchedule;
    }
    NES_INFO("QueryCatalog: Nothing to schedule.");
    return queriesToSchedule;
}

std::map<std::string, std::string> QueryCatalog::getAllQueries() {

    NES_INFO("QueryCatalog : get all queries");

    std::map<std::string, QueryCatalogEntryPtr> registeredQueries = getRegisteredQueries();

    std::map<std::string, std::string> result;
    for (auto [key, value] : registeredQueries) {
        result[key] = value->getQueryString();
    }

    NES_INFO("QueryCatalog : found " << result.size() << " queries in catalog.");
    return result;
}

bool QueryCatalog::registerAndQueueAddRequest(const std::string& queryString, const QueryPlanPtr queryPlan, const std::string& optimizationStrategyName) {
    std::unique_lock<std::mutex> lock(insertDeleteQuery);
    std::string queryId = queryPlan->getQueryId();
    NES_INFO("QueryCatalog: Registering query with id " << queryId);
    try {
        NES_INFO("QueryCatalog: Creating query catalog entry for query with id " << queryId);
        QueryCatalogEntryPtr queryCatalogEntry = std::make_shared<QueryCatalogEntry>(queryId, queryString, queryPlan, QueryStatus::Registered);
        queries[queryId] = queryCatalogEntry;
        NES_INFO("QueryCatalog: Adding query with id " << queryId << " to the scheduling queue");
        schedulingQueue.push_back(queryCatalogEntry);
        return true;
    } catch (const std::exception& exc) {
        NES_ERROR("QueryCatalog:_exception:" << exc.what() << " try to revert changes");
        NES_ERROR("QueryCatalog: Unable to process input request with: query id " << queryId << "queryString: " << queryString
                                                                                  << "\n strategy: " << optimizationStrategyName);
        markQueryAs(queryId, QueryStatus::Failed);
        return false;
    }
}

bool QueryCatalog::queueStopRequest(std::string queryId) {
    std::unique_lock<std::mutex> lock(insertDeleteQuery);
    NES_INFO("QueryCatalog: add query stop request to the scheduling queue.");

    NES_INFO("QueryCatalog: Locating a query with same id in the scheduling queue.");
    auto itr = std::find(schedulingQueue.begin(), schedulingQueue.end(), [queryId](QueryCatalogEntryPtr entry) {
        return entry->getQueryId() == queryId;
    });

    if (itr != schedulingQueue.end()) {
        NES_INFO("QueryCatalog: Found query with same id already present in the scheduling queue.");
        NES_INFO("QueryCatalog: Changing query status to Mark query for stop.");
        markQueryAs(queryId, QueryStatus::MarkedForStop);
    } else {
        QueryCatalogEntryPtr queryCatalogEntry = getQuery(queryId);
        QueryStatus currentStatus = queryCatalogEntry->getQueryStatus();
        if (currentStatus == QueryStatus::Stopped || currentStatus == QueryStatus::Failed) {
            NES_ERROR("QueryCatalog: Found query status already as " + queryCatalogEntry->getQueryStatusAsString() + ". Ignoring stop query request.");
            //throw exception that query is in an invalid state for this operation
        }
        NES_INFO("QueryCatalog: Changing query status to Mark query for stop.");
        queryCatalogEntry->setQueryStatus(QueryStatus::MarkedForStop);
        schedulingQueue.push_back(queryCatalogEntry);
    }
    return true;
}

bool QueryCatalog::deleteQuery(const std::string& queryId) {
    std::unique_lock<std::mutex> lock(insertDeleteQuery);
    if (!queryExists(queryId)) {
        NES_WARNING(
            "QueryCatalog: No deletion required! Query has neither been registered or deployed->" << queryId);
        return false;
    } else {
        NES_DEBUG("QueryCatalog: De-registering query ...");
        globalExecutionPlan->removeQuerySubPlans(queryId);
        if (getQuery(queryId)->getQueryStatus() == QueryStatus::Running) {
            NES_DEBUG("QueryCatalog: query is running, stopping it");
            markQueryAs(queryId, QueryStatus::Stopped);
        } else if (getQuery(queryId)->getQueryStatus() == QueryStatus::Stopped) {
            NES_DEBUG("QueryCatalog:deleteQuery: query already stopped");
        } else if (getQuery(queryId)->getQueryStatus() == QueryStatus::Scheduling) {
            NES_WARNING("QueryCatalog:deleteQuery: query is currently scheduled");
            return false;
        } else if (getQuery(queryId)->getQueryStatus() == QueryStatus::Registered) {
            NES_WARNING("QueryCatalog:deleteQuery:  query is just registered but not running");
        } else if (getQuery(queryId)->getQueryStatus() == QueryStatus::Failed) {
            NES_WARNING("QueryCatalog:deleteQuery: query status failed");
            return false;
        }

        NES_DEBUG("QueryCatalog: erase query " << queryId);
        size_t ret = queries.erase(queryId);
        if (ret == 0) {
            NES_WARNING("QueryCatalog: query does not exists");
            return false;
        } else if (ret > 1) {
            NES_ERROR("QueryCatalog: query exists two times in the catalog, this should not happen");
            return false;
        }
        return true;
    }
}

void QueryCatalog::markQueryAs(std::string queryId, QueryStatus queryStatus) {
    //TODO: check if a mutex is required
    NES_DEBUG("QueryCatalog: mark query with id " << queryId << " as " << queryStatus);
    queries[queryId]->setQueryStatus(queryStatus);
}

bool QueryCatalog::isQueryRunning(std::string queryId) {
    NES_DEBUG(
        "QueryCatalog: test if query started with id " << queryId << " running=" << queries[queryId]->getQueryStatus());
    return queries[queryId]->getQueryStatus() == QueryStatus::Running;
}

std::map<std::string, QueryCatalogEntryPtr> QueryCatalog::getRegisteredQueries() {
    NES_DEBUG("QueryCatalog: return registered queries=" << printQueries());
    return queries;
}

QueryCatalogEntryPtr QueryCatalog::getQuery(std::string queryId) {
    NES_DEBUG("QueryCatalog: getQuery with id " << queryId);
    return queries[queryId];
}

bool QueryCatalog::queryExists(std::string queryId) {
    NES_DEBUG(
        "QueryCatalog: queryExists with id=" << queryId << " registered queries=" << printQueries());
    if (queries.count(queryId) > 0) {
        NES_DEBUG("QueryCatalog: query with id " << queryId << " exists");
        return true;
    } else {
        NES_DEBUG("QueryCatalog: query with id " << queryId << " does not exist");
        return false;
    }
}

std::map<std::string, QueryCatalogEntryPtr> QueryCatalog::getQueries(QueryStatus requestedStatus) {
    NES_DEBUG(
        "QueryCatalog: getQueriesWithStatus() registered queries=" << printQueries());

    std::map<std::string, QueryCatalogEntryPtr> runningQueries;
    for (auto q : queries) {
        if (q.second->getQueryStatus() == requestedStatus) {
            runningQueries.insert(q);
        }
    }
    return runningQueries;
}

void QueryCatalog::clearQueries() {
    NES_DEBUG("QueryCatalog: clear query catalog");
    queries.clear();
    assert(queries.size() == 0);
}

std::string QueryCatalog::printQueries() {
    std::stringstream ss;
    for (auto q : queries) {
        ss << "queryID=" << q.first << " running=" << q.second->getQueryStatus() << std::endl;
    }
    return ss.str();
}

}// namespace NES

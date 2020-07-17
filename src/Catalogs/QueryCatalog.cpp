#include <Catalogs/QueryCatalog.hpp>
#include <Exceptions/InvalidArgumentException.hpp>
#include <Exceptions/InvalidQueryStatusException.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <string>

namespace NES {

QueryCatalog::QueryCatalog() : insertQueryRequest(), newRequestAvailable(false) {
    NES_DEBUG("QueryCatalog()");
}

std::map<std::string, std::string> QueryCatalog::getQueriesWithStatus(std::string status) {

    NES_INFO("QueryCatalog : fetching all queries with status " << status);
    std::transform(status.begin(), status.end(), status.begin(), ::toupper);
    if (stringToQueryStatusMap.find(status) == stringToQueryStatusMap.end()) {
        throw InvalidArgumentException("status", status);
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

std::vector<QueryCatalogEntry> QueryCatalog::getQueriesToSchedule() {
    std::unique_lock<std::mutex> lock(insertQueryRequest);
    NES_INFO("QueryCatalog: Fetching Queries to Schedule");
    std::vector<QueryCatalogEntry> queriesToSchedule;
    if (!schedulingQueue.empty()) {
        int64_t currentBatchSize = 1;
        int64_t totalQueriesToSchedule = schedulingQueue.size();
        //Prepare a batch of queries to schedule
        while (currentBatchSize <= batchSize || currentBatchSize == totalQueriesToSchedule) {
            queriesToSchedule.push_back(schedulingQueue.front()->copy());
            schedulingQueue.pop_front();
            currentBatchSize++;
        }
        NES_INFO("QueryCatalog: Scheduling " << queriesToSchedule.size() << " queries.");
        setNewRequestAvailable(!schedulingQueue.empty());
        return queriesToSchedule;
    }
    NES_INFO("QueryCatalog: Nothing to schedule.");
    setNewRequestAvailable(!schedulingQueue.empty());
    return queriesToSchedule;
}

std::map<std::string, std::string> QueryCatalog::getAllQueries() {

    NES_INFO("QueryCatalog : get all queries");
    std::map<std::string, QueryCatalogEntryPtr> registeredQueries = getAllQueryCatalogEntries();
    std::map<std::string, std::string> result;
    for (auto [key, value] : registeredQueries) {
        result[key] = value->getQueryString();
    }
    NES_INFO("QueryCatalog : found " << result.size() << " queries in catalog.");
    return result;
}

bool QueryCatalog::registerAndQueueAddRequest(const std::string& queryString, const QueryPlanPtr queryPlan, const std::string& optimizationStrategyName) {
    std::unique_lock<std::mutex> lock(insertQueryRequest);
    std::string queryId = queryPlan->getQueryId();
    NES_INFO("QueryCatalog: Registering query with id " << queryId);
    try {
        NES_INFO("QueryCatalog: Creating query catalog entry for query with id " << queryId);
        QueryCatalogEntryPtr queryCatalogEntry = std::make_shared<QueryCatalogEntry>(queryId, queryString, optimizationStrategyName, queryPlan, QueryStatus::Registered);
        queries[queryId] = queryCatalogEntry;
        NES_INFO("QueryCatalog: Adding query with id " << queryId << " to the scheduling queue");
        schedulingQueue.push_back(queryCatalogEntry);
        NES_INFO("QueryCatalog: Marking that new request is available to be scheduled");
        setNewRequestAvailable(true);
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
    std::unique_lock<std::mutex> lock(insertQueryRequest);
    NES_INFO("QueryCatalog: Queue query stop request to the scheduling queue.");
    NES_INFO("QueryCatalog: Locating a query with same id in the scheduling queue.");
    QueryCatalogEntryPtr queryCatalogEntry = getQueryCatalogEntry(queryId);
    auto itr = std::find(schedulingQueue.begin(), schedulingQueue.end(), queryCatalogEntry);
    if (itr != schedulingQueue.end()) {
        NES_INFO("QueryCatalog: Found query with same id already present in the scheduling queue.");
        NES_INFO("QueryCatalog: Changing query status to Mark query for stop.");
        markQueryAs(queryId, QueryStatus::MarkedForStop);
    } else {
        QueryStatus currentStatus = queryCatalogEntry->getQueryStatus();
        if (currentStatus == QueryStatus::Stopped || currentStatus == QueryStatus::Failed) {
            NES_ERROR("QueryCatalog: Found query status already as " + queryCatalogEntry->getQueryStatusAsString() + ". Ignoring stop query request.");
            throw InvalidQueryStatusException({QueryStatus::Scheduling, QueryStatus::Registered, QueryStatus::Running}, currentStatus);
        }
        NES_INFO("QueryCatalog: Changing query status to Mark query for stop.");
        queryCatalogEntry->setQueryStatus(QueryStatus::MarkedForStop);
        schedulingQueue.push_back(queryCatalogEntry);
    }
    NES_INFO("QueryCatalog: Marking that new request is available to be scheduled");
    setNewRequestAvailable(true);
    return true;
}

void QueryCatalog::markQueryAs(std::string queryId, QueryStatus newStatus) {

    NES_DEBUG("QueryCatalog: mark query with id " << queryId << " as " << newStatus);
    QueryCatalogEntryPtr queryCatalogEntry = getQueryCatalogEntry(queryId);
    QueryStatus oldStatus = queryCatalogEntry->getQueryStatus();
    if (oldStatus == QueryStatus::MarkedForStop && newStatus == QueryStatus::Running) {
        NES_WARNING("QueryCatalog: Skip setting status of a query marked for stop as running.");
    } else {
        queries[queryId]->setQueryStatus(newStatus);
    }
}

bool QueryCatalog::isQueryRunning(std::string queryId) {
    NES_DEBUG("QueryCatalog: test if query started with id " << queryId << " running=" << queries[queryId]->getQueryStatus());
    return queries[queryId]->getQueryStatus() == QueryStatus::Running;
}

std::map<std::string, QueryCatalogEntryPtr> QueryCatalog::getAllQueryCatalogEntries() {
    NES_DEBUG("QueryCatalog: return registered queries=" << printQueries());
    return queries;
}

QueryCatalogEntryPtr QueryCatalog::getQueryCatalogEntry(std::string queryId) {
    NES_DEBUG("QueryCatalog: getQueryCatalogEntry with id " << queryId);
    return queries[queryId];
}

bool QueryCatalog::queryExists(std::string queryId) {
    NES_DEBUG("QueryCatalog: queryExists with id=" << queryId << " registered queries=" << printQueries());
    if (queries.count(queryId) > 0) {
        NES_DEBUG("QueryCatalog: query with id " << queryId << " exists");
        return true;
    } else {
        NES_DEBUG("QueryCatalog: query with id " << queryId << " does not exist");
        return false;
    }
}

std::map<std::string, QueryCatalogEntryPtr> QueryCatalog::getQueries(QueryStatus requestedStatus) {
    NES_DEBUG("QueryCatalog: getQueriesWithStatus() registered queries=" << printQueries());
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

bool QueryCatalog::isNewRequestAvailable() const {
    return newRequestAvailable;
}

void QueryCatalog::setNewRequestAvailable(bool newRequestAvailable) {
    this->newRequestAvailable = newRequestAvailable;
}

}// namespace NES

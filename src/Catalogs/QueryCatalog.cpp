#include <Catalogs/QueryCatalog.hpp>
#include <Exceptions/InvalidArgumentException.hpp>
#include <Exceptions/InvalidQueryStatusException.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <string>

namespace NES {

QueryCatalog::QueryCatalog() : queryStatus(), queryRequest(){
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

QueryCatalogEntryPtr QueryCatalog::addNewQueryRequest(const std::string& queryString, const QueryPlanPtr queryPlan, const std::string& optimizationStrategyName) {
    std::unique_lock<std::mutex> lock(queryRequest);
    std::string queryId = queryPlan->getQueryId();
    NES_INFO("QueryCatalog: Registering query with id " << queryId);
    NES_INFO("QueryCatalog: Creating query catalog entry for query with id " << queryId);
    QueryCatalogEntryPtr queryCatalogEntry = std::make_shared<QueryCatalogEntry>(queryId, queryString, optimizationStrategyName, queryPlan, QueryStatus::Registered);
    queries[queryId] = queryCatalogEntry;
    return queryCatalogEntry;
}

QueryCatalogEntryPtr QueryCatalog::addQueryStopRequest(std::string queryId) {
    std::unique_lock<std::mutex> lock(queryRequest);
    NES_INFO("QueryCatalog: Validating with old query status.");
    QueryCatalogEntryPtr queryCatalogEntry = getQueryCatalogEntry(queryId);
    QueryStatus currentStatus = queryCatalogEntry->getQueryStatus();
    if (currentStatus == QueryStatus::Stopped || currentStatus == QueryStatus::Failed) {
        NES_ERROR("QueryCatalog: Found query status already as " + queryCatalogEntry->getQueryStatusAsString() + ". Ignoring stop query request.");
        throw InvalidQueryStatusException({QueryStatus::Scheduling, QueryStatus::Registered, QueryStatus::Running}, currentStatus);
    }
    NES_INFO("QueryCatalog: Changing query status to Mark query for stop.");
    markQueryAs(queryId, QueryStatus::MarkedForStop);
    return queryCatalogEntry;
}

void QueryCatalog::markQueryAs(std::string queryId, QueryStatus newStatus) {
    std::unique_lock<std::mutex> lock(queryStatus);
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

}// namespace NES

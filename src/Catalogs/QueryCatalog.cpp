#include <API/QueryId.hpp>
#include <Catalogs/QueryCatalog.hpp>
#include <Exceptions/InvalidArgumentException.hpp>
#include <Exceptions/InvalidQueryStatusException.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <string>

namespace NES {

QueryCatalog::QueryCatalog() : catalogMutex() {
    NES_DEBUG("QueryCatalog()");
}

QueryCatalog::~QueryCatalog() {
    NES_DEBUG("~QueryCatalog()");
}

std::map<uint64_t, std::string> QueryCatalog::getQueriesWithStatus(std::string status) {
    std::unique_lock lock(catalogMutex);
    NES_INFO("QueryCatalog : fetching all queries with status " << status);
    std::transform(status.begin(), status.end(), status.begin(), ::toupper);
    if (stringToQueryStatusMap.find(status) == stringToQueryStatusMap.end()) {
        throw InvalidArgumentException("status", status);
    }
    QueryStatus queryStatus = stringToQueryStatusMap[status];
    std::map<uint64_t, QueryCatalogEntryPtr> queries = getQueries(queryStatus);
    std::map<uint64_t, std::string> result;
    for (auto [key, value] : queries) {
        result[key] = value->getQueryString();
    }
    NES_INFO("QueryCatalog : found " << result.size() << " all queries with status " << status);
    return result;
}

std::map<uint64_t, std::string> QueryCatalog::getAllQueries() {
    std::unique_lock lock(catalogMutex);
    NES_INFO("QueryCatalog : get all queries");
    std::map<uint64_t, QueryCatalogEntryPtr> registeredQueries = getAllQueryCatalogEntries();
    std::map<uint64_t, std::string> result;
    for (auto [key, value] : registeredQueries) {
        result[key] = value->getQueryString();
    }
    NES_INFO("QueryCatalog : found " << result.size() << " queries in catalog.");
    return result;
}

QueryCatalogEntryPtr QueryCatalog::addNewQueryRequest(const std::string& queryString, const QueryPlanPtr queryPlan, const std::string& optimizationStrategyName) {
    std::unique_lock lock(catalogMutex);
    QueryId queryId = queryPlan->getQueryId();
    NES_INFO("QueryCatalog: Creating query catalog entry for query with id " << queryId);
    QueryCatalogEntryPtr queryCatalogEntry = std::make_shared<QueryCatalogEntry>(queryId, queryString, optimizationStrategyName, queryPlan, QueryStatus::Registered);
    queries[queryId] = queryCatalogEntry;
    return queryCatalogEntry;
}

QueryCatalogEntryPtr QueryCatalog::addQueryStopRequest(QueryId queryId) {
    std::unique_lock lock(catalogMutex);
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

void QueryCatalog::markQueryAs(QueryId queryId, QueryStatus newStatus) {
    std::unique_lock lock(catalogMutex);
    NES_DEBUG("QueryCatalog: mark query with id " << queryId << " as " << newStatus);
    QueryCatalogEntryPtr queryCatalogEntry = getQueryCatalogEntry(queryId);
    QueryStatus oldStatus = queryCatalogEntry->getQueryStatus();
    if (oldStatus == QueryStatus::MarkedForStop && newStatus == QueryStatus::Running) {
        NES_WARNING("QueryCatalog: Skip setting status of a query marked for stop as running.");
    } else {
        queries[queryId]->setQueryStatus(newStatus);
    }
}

bool QueryCatalog::isQueryRunning(QueryId queryId) {
    std::unique_lock lock(catalogMutex);
    NES_DEBUG("QueryCatalog: test if query started with id " << queryId << " running=" << queries[queryId]->getQueryStatus());
    return queries[queryId]->getQueryStatus() == QueryStatus::Running;
}

std::map<uint64_t, QueryCatalogEntryPtr> QueryCatalog::getAllQueryCatalogEntries() {
    std::unique_lock lock(catalogMutex);
    NES_DEBUG("QueryCatalog: return registered queries=" << printQueries());
    return queries;
}

QueryCatalogEntryPtr QueryCatalog::getQueryCatalogEntry(QueryId queryId) {
    std::unique_lock lock(catalogMutex);
    NES_TRACE("QueryCatalog: getQueryCatalogEntry with id " << queryId);
    return queries[queryId];
}

bool QueryCatalog::queryExists(QueryId queryId) {
    std::unique_lock lock(catalogMutex);
    NES_DEBUG("QueryCatalog: queryExists with id=" << queryId << " registered queries=" << printQueries());
    if (queries.count(queryId) > 0) {
        NES_DEBUG("QueryCatalog: query with id " << queryId << " exists");
        return true;
    } else {
        NES_DEBUG("QueryCatalog: query with id " << queryId << " does not exist");
        return false;
    }
}

std::map<uint64_t, QueryCatalogEntryPtr> QueryCatalog::getQueries(QueryStatus requestedStatus) {
    std::unique_lock lock(catalogMutex);
    NES_DEBUG("QueryCatalog: getQueriesWithStatus() registered queries=" << printQueries());
    std::map<uint64_t, QueryCatalogEntryPtr> runningQueries;
    for (auto q : queries) {
        if (q.second->getQueryStatus() == requestedStatus) {
            runningQueries.insert(q);
        }
    }
    return runningQueries;
}

void QueryCatalog::clearQueries() {
    std::unique_lock lock(catalogMutex);
    NES_DEBUG("QueryCatalog: clear query catalog");
    queries.clear();
}

std::string QueryCatalog::printQueries() {
    std::unique_lock lock(catalogMutex);
    std::stringstream ss;
    for (auto q : queries) {
        ss << "queryID=" << q.first << " running=" << q.second->getQueryStatus() << std::endl;
    }
    return ss.str();
}

}// namespace NES

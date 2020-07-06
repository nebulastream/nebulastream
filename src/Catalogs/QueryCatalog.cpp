#include <Catalogs/QueryCatalog.hpp>
#include <Exceptions/ErrorWhileRevertingChanges.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Services/OptimizerService.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <string>
#include <unordered_set>
namespace NES {

QueryCatalog::QueryCatalog(TopologyManagerPtr topologyManager, StreamCatalogPtr streamCatalog, GlobalExecutionPlanPtr globalExecutionPlan)
    : topologyManager(topologyManager), streamCatalog(streamCatalog), globalExecutionPlan(globalExecutionPlan), insertDeleteQuery() {
    NES_DEBUG("QueryCatalog()");
}

std::map<string, string> QueryCatalog::getQueriesWithStatus(std::string status) {

    NES_INFO("QueryCatalog : fetching all queries with status " << status);

    std::transform(status.begin(), status.end(), status.begin(), ::toupper);

    if (stringToQueryStatusMap.find(status) == stringToQueryStatusMap.end()) {
        throw std::invalid_argument("Unknown query status " + status);
    }

    QueryStatus queryStatus = stringToQueryStatusMap[status];
    map<string, QueryCatalogEntryPtr> queries = getQueries(queryStatus);

    map<string, string> result;
    for (auto [key, value] : queries) {
        result[key] = value->getQueryString();
    }

    NES_INFO("QueryCatalog : found " << result.size() << " all queries with status " << status);
    return result;
}

std::map<std::string, std::string> QueryCatalog::getAllRegisteredQueries() {

    NES_INFO("QueryCatalog : get all registered queries");

    map<string, QueryCatalogEntryPtr> registeredQueries = getRegisteredQueries();

    map<string, string> result;
    for (auto [key, value] : registeredQueries) {
        result[key] = value->getQueryString();
    }

    NES_INFO("QueryCatalog : found " << result.size() << " registered queries");
    return result;
}

string QueryCatalog::registerQuery(const string& queryString, const string& optimizationStrategyName) {
    std::unique_lock<std::mutex> lock(insertDeleteQuery);
    NES_DEBUG(
        "QueryCatalog: Registering query " << queryString << " with strategy " << optimizationStrategyName);

    if (queryString.find("Stream(") != std::string::npos || queryString.find("Schema::create()") != std::string::npos) {

        NES_ERROR("QueryCatalog: queries are not allowed to specify schemas anymore.");
        throw Exception("Queries are not allowed to define schemas anymore");
    }

    string queryId;
    try {
        QueryPtr query = UtilityFunctions::createQueryFromCodeString(queryString);
        OptimizerServicePtr optimizerService = std::make_shared<OptimizerService>(topologyManager, streamCatalog, globalExecutionPlan);
        auto queryPlan = query->getQueryPlan();
        queryId = queryPlan->getQueryId();
        if (queryId == "") {
            NES_ERROR("QueryCatalog::registerQuery: cannot register query without id");
            throw;
        }
        GlobalExecutionPlanPtr executionPlan = optimizerService->updateGlobalExecutionPlan(queryPlan, optimizationStrategyName);
        if (!executionPlan) {
            NES_ERROR("QueryCatalog::registerQuery updateGlobalExecutionPlan failed");
            return "ERROR create execution plan";
        }
        NES_DEBUG("QueryCatalog: Final Execution Plan =" << executionPlan->getAsString());

        QueryCatalogEntryPtr entry = std::make_shared<QueryCatalogEntry>(queryId, queryString, queryPlan, QueryStatus::Registered);
        queries[queryId] = entry;
        if (queries.find(queryId)->second != entry) {
            NES_ERROR("QueryCatalog::registerQuery insert into query failed");
            //revert changes
            if (!globalExecutionPlan->removeQuerySubPlans(queryId)) {
                //this a severe error so we should terminate
                throw ErrorWhileRevertingChanges();
            }
            markQueryAs(queryId, QueryStatus::Failed);
            return "ERROR insertMap";
        }
        NES_DEBUG("number of queries after insert=" << queries.size());
        return queryId;
    } catch (const ErrorWhileRevertingChanges e) {
        throw;
    } catch (const std::exception& exc) {
        NES_ERROR("QueryCatalog:_exception:" << exc.what() << " try to revert changes");
        //revert changes
        if (!globalExecutionPlan->removeQuerySubPlans(queryId)) {
            //this a severe error so we should terminate
            throw ErrorWhileRevertingChanges();
        }
        markQueryAs(queryId, QueryStatus::Failed);

        NES_ERROR(
            "QueryCatalog: Unable to process input request with: queryString: " << queryString << "\n strategy: "
                                                                                << optimizationStrategyName);
        NES_ERROR("QueryCatalog::registerQuery insert into query failed with " << exc.what());
        return "ERROR QueryCatalog:exception";
    }
}

bool QueryCatalog::deleteQuery(const string& queryId) {
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
}// namespace NES

void QueryCatalog::markQueryAs(string queryId, QueryStatus queryStatus) {
    //TODO: check if a mutex is required
    NES_DEBUG("QueryCatalog: mark query with id " << queryId << " as " << queryStatus);
    queries[queryId]->setQueryStatus(queryStatus);
}

bool QueryCatalog::isQueryRunning(string queryId) {
    NES_DEBUG(
        "QueryCatalog: test if query started with id " << queryId << " running=" << queries[queryId]->getQueryStatus());
    return queries[queryId]->getQueryStatus() == QueryStatus::Running;
}

map<string, QueryCatalogEntryPtr> QueryCatalog::getRegisteredQueries() {
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

map<string, QueryCatalogEntryPtr> QueryCatalog::getQueries(QueryStatus requestedStatus) {
    NES_DEBUG(
        "QueryCatalog: getQueriesWithStatus() registered queries=" << printQueries());

    map<string, QueryCatalogEntryPtr> runningQueries;
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
    stringstream ss;
    for (auto q : queries) {
        ss << "queryID=" << q.first << " running=" << q.second->getQueryStatus() << endl;
    }
    return ss.str();
}

}// namespace NES

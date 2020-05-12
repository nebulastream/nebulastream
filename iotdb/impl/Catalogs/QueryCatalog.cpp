#include <Catalogs/QueryCatalog.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <string>
#include <Services/OptimizerService.hpp>
#include <bits/stdc++.h>

namespace NES {

QueryCatalog& QueryCatalog::instance() {
    static QueryCatalog instance;
    return instance;
}

string QueryCatalog::registerQuery(const string& queryString,
                                   const string& optimizationStrategyName) {
    NES_DEBUG(
        "QueryCatalog: Registering query " << queryString << " with strategy " << optimizationStrategyName);

    if (queryString.find("Stream(") != std::string::npos || queryString.find("Schema::create()") != std::string::npos) {

        NES_ERROR("QueryCatalog: queries are not allowed to specify schemas anymore.");
        throw Exception("Queries are not allowed to define schemas anymore");
    }
    try {
        InputQueryPtr inputQueryPtr = UtilityFunctions::createQueryFromCodeString(queryString);
        SchemaPtr schema = inputQueryPtr->getSourceStream()->getSchema();

        NESExecutionPlanPtr nesExecutionPtr = OptimizerService::getInstance()->getExecutionPlan(
            inputQueryPtr, optimizationStrategyName);

        NES_DEBUG(
            "QueryCatalog: Final Execution Plan =" << nesExecutionPtr->getTopologyPlanString());

        boost::uuids::basic_random_generator<boost::mt19937> gen;
        boost::uuids::uuid u = gen();
        std::string queryId = boost::uuids::to_string(u);//TODO: I am not sure this will not create a unique one

        QueryCatalogEntryPtr entry = std::make_shared<QueryCatalogEntry>(
            queryId, queryString, inputQueryPtr, nesExecutionPtr, QueryStatus::Registered);

        queries[queryId] = entry;
        NES_DEBUG("number of queries after insert=" << queries.size());

        return queryId;
    } catch (const std::exception& exc) {
        NES_ERROR("QueryCatalog:_exception:" << exc.what());
        NES_ERROR(
            "QueryCatalog: Unable to process input request with: queryString: " << queryString << "\n strategy: "
                                                                                << optimizationStrategyName);
        throw;
    } catch (...) {
        NES_ERROR(
            "QueryCatalog: Unable to process input request with: queryString: " << queryString << "\n strategy: "
                                                                                << optimizationStrategyName);
        return "";
    }
}

bool QueryCatalog::deleteQuery(const string& queryId) {
    if (!queryExists(queryId)) {
        NES_DEBUG(
            "QueryCatalog: No deletion required! Query has neither been registered or deployed->" << queryId);
        return false;
    } else {
        NES_DEBUG("QueryCatalog: De-registering query ...");
        NESExecutionPlanPtr execPlan = QueryCatalog::instance().getQuery(queryId)->getNesPlanPtr();
        execPlan->freeResources();
        if (QueryCatalog::instance().getQuery(queryId)->getQueryStatus() == QueryStatus::Running) {
            NES_DEBUG("QueryCatalog: query is running, stopping it");
            QueryCatalog::instance().markQueryAs(queryId, QueryStatus::Stopped);
        }
        NES_DEBUG("QueryCatalog: erase query " << queryId);
        std::map<string, QueryCatalogEntryPtr>::iterator it;
        it = queries.find(queryId);
        queries.erase(it);
        return true;
    }
}

std::vector<NESTopologyEntryPtr> QueryCatalog::getExecutionNodesToQuery(string queryId) {
    return queriesToExecNodeMap[queryId];
}

void QueryCatalog::addExecutionNodesToQuery(string queryId, std::vector<NESTopologyEntryPtr> nodes) {
    std::vector<NESTopologyEntryPtr>& vec = queriesToExecNodeMap[queryId];
    std::copy(nodes.begin(), nodes.end(), std::back_inserter(vec));
}

void QueryCatalog::removeExecutionNodesToQuery(string queryId, std::vector<NESTopologyEntryPtr> nodes) {
    std::vector<NESTopologyEntryPtr>& vec = queriesToExecNodeMap[queryId];

    std::unordered_multiset<NESTopologyEntryPtr> st;
    st.insert(vec.begin(), vec.end());
    st.insert(nodes.begin(), nodes.end());
    auto predicate = [&st](const NESTopologyEntryPtr& k) {
        return st.count(k) > 1;
    };
    vec.erase(std::remove_if(vec.begin(), vec.end(), predicate), vec.end());
}

void QueryCatalog::markQueryAs(string queryId, QueryStatus queryStatus) {
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

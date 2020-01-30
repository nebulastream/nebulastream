#include <Catalogs/QueryCatalog.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <string>
#include <Services/OptimizerService.hpp>

namespace NES {

QueryCatalog& QueryCatalog::instance() {
  static QueryCatalog instance;
  return instance;
}

string QueryCatalog::register_query(const string &queryString,
                                    const string &optimizationStrategyName) {
  NES_DEBUG(
      "QueryCatalog: Registering query " << queryString << " with strategy " << optimizationStrategyName);

  if (queryString.find("Stream(") != std::string::npos) {
    NES_ERROR(
        "QueryCatalog: queries are not allowed to specify streams anymore.")
    throw Exception("Queries are not allowed to define streams anymore");
  }
  if (queryString.find("Schema::create()") != std::string::npos) {
    NES_ERROR(
        "QueryCatalog: queries are not allowed to specify schemas anymore.")
    throw Exception("Queries are not allowed to define schemas anymore");
  }

  try {
    InputQueryPtr inputQueryPtr = UtilityFunctions::createQueryFromCodeString(
        queryString);
    Schema schema = inputQueryPtr->getSourceStream()->getSchema();

    NESExecutionPlanPtr nesExecutionPtr = OptimizerService::instance().getExecutionPlan(
        inputQueryPtr, optimizationStrategyName);

    NES_DEBUG(
        "QueryCatalog: Final Execution Plan =" << nesExecutionPtr->getTopologyPlanString())

    boost::uuids::basic_random_generator<boost::mt19937> gen;
    boost::uuids::uuid u = gen();
    std::string queryId = boost::uuids::to_string(u);  //TODO: I am not sure this will not create a unique one

    QueryCatalogEntryPtr entry = std::make_shared<QueryCatalogEntry>(
        queryId, inputQueryPtr, nesExecutionPtr, false);

    queries[queryId] = entry;
    NES_DEBUG("number of quieries after insert=" << queries.size())

    return queryId;
  } catch (...) {
    NES_ERROR(
        "QueryCatalog: Unable to process input request with: queryString: " << queryString << "\n strategy: " << optimizationStrategyName);
    assert(0);
    return nullptr;
  }
}

bool QueryCatalog::deregister_query(const string &queryId) {
  if (!testIfQueryExists(queryId)) {
    NES_DEBUG(
        "QueryCatalog: No deletion required! Query has neither been registered or deployed->" << queryId);
    return false;
  } else {
    NES_DEBUG("QueryCatalog: De-registering query ...");
    NESExecutionPlanPtr execPlan = QueryCatalog::instance().getQuery(queryId)
        ->nesPlanPtr;
    execPlan->freeResources();
    if (QueryCatalog::instance().getQuery(queryId)->running == true) {
      NES_DEBUG("QueryCatalog: query is running, stopping it");
      QueryCatalog::instance().stopQuery(queryId);
    }
    NES_DEBUG("QueryCatalog: erase query " << queryId);
    std::map<string, QueryCatalogEntryPtr>::iterator it;
    it = queries.find(queryId);
    queries.erase(it);
    return true;
  }
}

void QueryCatalog::startQuery(string queryId) {
  NES_DEBUG("QueryCatalog: start query with id " << queryId)
  queries[queryId]->running = true;
}

void QueryCatalog::stopQuery(string queryId) {
  NES_DEBUG("QueryCatalog: stop query with id " << queryId)
  queries[queryId]->running = false;
}

bool QueryCatalog::testIfQueryStarted(string queryId) {
  NES_DEBUG(
      "QueryCatalog: test if query started with id " << queryId << " running=" << queries[queryId]->running)
  return queries[queryId]->running;
}

map<string, QueryCatalogEntryPtr> QueryCatalog::getRegisteredQueries() {
  NES_DEBUG("QueryCatalog: return registered queries=" << printQueries())
  return queries;
}

QueryCatalogEntryPtr QueryCatalog::getQuery(std::string queryId) {
  NES_DEBUG("QueryCatalog: getQuery with id " << queryId)
  return queries[queryId];
}

bool QueryCatalog::testIfQueryExists(std::string queryId) {
  NES_DEBUG(
      "QueryCatalog: testIfQueryExists with id=" << queryId << " registered queries=" << printQueries())
  if (queries.count(queryId) > 0) {
    NES_DEBUG("QueryCatalog: query with id " << queryId << " exists")
    return true;
  } else {
    NES_DEBUG("QueryCatalog: query with id " << queryId << " does not exist")
    return false;
  }
}

map<string, QueryCatalogEntryPtr> QueryCatalog::getRunningQueries() {
  NES_DEBUG(
      "QueryCatalog: getRunningQueries() registered queries=" << printQueries())

  map<string, QueryCatalogEntryPtr> runningQueries;
  for (auto q : queries) {
    if (q.second->running == true) {
      runningQueries.insert(q);
    }
  }
  return runningQueries;
}

void QueryCatalog::clearQueries() {
  NES_DEBUG("QueryCatalog: clear query catalog")
  queries.clear();
  assert(queries.size() == 0);
}

std::string QueryCatalog::printQueries() {
  stringstream ss;
  for (auto q : queries) {
    ss << "queryID=" << q.first << " running=" << q.second->running << endl;
  }
  return ss.str();
}

}

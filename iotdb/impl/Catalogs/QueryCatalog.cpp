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
  NES_INFO(
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
    Schema schema = inputQueryPtr->source_stream->getSchema();

    NESExecutionPlanPtr nPtr = OptimizerService::instance().getExecutionPlan(
        inputQueryPtr, optimizationStrategyName);
//    const NESExecutionPlan &kExecutionPlan = optimizerService.getExecutionPlan(
//        inputQueryPtr, optimizationStrategyName);

    NES_DEBUG(
        "QueryCatalog: Final Execution Plan =" << nPtr->getTopologyPlanString())

    boost::uuids::basic_random_generator<boost::mt19937> gen;
    boost::uuids::uuid u = gen();
    std::string queryId = boost::uuids::to_string(u);  //TODO: I am not sure this will not create a unique one

    QueryCatalogEntryPtr entry = std::make_shared<QueryCatalogEntry>(
        queryId, inputQueryPtr, nPtr, schema, false);
//    tuple<Schema, NESExecutionPlan> t = std::make_tuple(schema, kExecutionPlan);
    //TODO: check if query already exists
    queries[queryId] = entry;

    return queryId;
  } catch (...) {
    NES_ERROR(
        "QueryCatalog: Unable to process input request with: queryString: " << queryString << "\n strategy: " << optimizationStrategyName);
    return nullptr;
  }
}

bool QueryCatalog::deregister_query(const string &queryId) {
  if (testIfQueryExists(queryId)) {
    NES_INFO(
        "QueryCatalog: No deletion required! Query has neither been registered or deployed->" << queryId);
    return false;
  } else if (testIfQueryExists(queryId)) {
    NES_INFO("QueryCatalog: De-registering query ...");
    NESExecutionPlanPtr execPlan = QueryCatalog::instance().getQuery(queryId)
        ->nesPlanPtr;
    execPlan->freeResources();
    if (QueryCatalog::instance().getQuery(queryId)->running == true) {
      NES_INFO("QueryCatalog: query is running, stopping it");
      QueryCatalog::instance().stopQuery(queryId);
    }
    std::map<string, QueryCatalogEntryPtr>::iterator it;
    it = queries.find(queryId);
    queries.erase(it);

    return true;
  }
  return false;
}
bool QueryCatalog::startQuery(string queryId) {
  NES_NOT_IMPLEMENTED
}

bool QueryCatalog::stopQuery(string queryId) {
  NES_NOT_IMPLEMENTED
}

map<string, QueryCatalogEntryPtr> QueryCatalog::getRegisteredQueries() {
  NES_NOT_IMPLEMENTED
}

QueryCatalogEntryPtr QueryCatalog::getQuery(std::string queryID) {
  NES_NOT_IMPLEMENTED
}

bool QueryCatalog::testIfQueryExists(std::string queryID) {
  NES_NOT_IMPLEMENTED
}

map<string, QueryCatalogEntryPtr> QueryCatalog::getRunningQueries() {
  NES_NOT_IMPLEMENTED
}

void QueryCatalog::clearQueries() {
  NES_NOT_IMPLEMENTED
}

}

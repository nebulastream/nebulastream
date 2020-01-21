#include <Catalogs/QueryCatalog.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <string>
#include <Services/OptimizerService.hpp>

namespace NES {

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

    NESExecutionPlanPtr nPtr = OptimizerService::instance().getExecutionPlan(inputQueryPtr, optimizationStrategyName);
//    const NESExecutionPlan &kExecutionPlan = optimizerService.getExecutionPlan(
//        inputQueryPtr, optimizationStrategyName);

    NES_DEBUG(
        "QueryCatalog: Final Execution Plan =" << nPtr->getTopologyPlanString())

    boost::uuids::basic_random_generator<boost::mt19937> gen;
    boost::uuids::uuid u = gen();
    std::string queryId = boost::uuids::to_string(u);//TODO: I am not sure this will not create a unique one

    QueryCatalogEntry entry(queryId, inputQueryPtr, nPtr, schema, false);
//    tuple<Schema, NESExecutionPlan> t = std::make_tuple(schema, kExecutionPlan);
    queries.insert(queryId, entry);

    return queryId;
  } catch (...) {
    NES_ERROR(
        "Unable to process input request with: queryString: " << queryString << "\n strategy: " << optimizationStrategyName);
    return nullptr;
  }
}

bool QueryCatalog::deregister_query(const string &queryId) {
  bool out = false;
  if (this->registeredQueries.find(queryId) == this->registeredQueries.end()
      && this->runningQueries.find(queryId) == this->runningQueries.end()) {
    NES_INFO(
        "CoordinatorService: No deletion required! Query has neither been registered or deployed->" << queryId);
  } else if (this->registeredQueries.find(queryId)
      != this->registeredQueries.end()) {
    // Query is registered, but not running -> just remove from registered queries
    get<1>(this->registeredQueries.at(queryId)).freeResources();
    this->registeredQueries.erase(queryId);
    NES_INFO(
        "CoordinatorService: Query was registered and has been successfully removed -> " << queryId);
  } else {
    NES_INFO("CoordinatorService: De-registering running query..");
    //Query is running -> stop query locally if it is running and free resources
    get<1>(this->runningQueries.at(queryId)).freeResources();
    this->runningQueries.erase(queryId);
    NES_INFO("CoordinatorService:  successfully removed query " << queryId);
    out = true;
  }
  return out;
}

}

#ifndef INCLUDE_CATALOGS_QUERYCATALOG_HPP_
#define INCLUDE_CATALOGS_QUERYCATALOG_HPP_

#include <unordered_map>
#include <string>
#include <Optimizer/NESExecutionPlan.hpp>
#include <Catalogs/QueryCatalog.hpp>

using namespace std;
namespace NES {

/**
 * @brief class to handle the entry in the query catalog
 * @param queryId: id of the query (is also the key in the queries map)
 * @param inputQuery: a pointer to the input query
 * @param nesPlanPtr: a pointer to the generated nes execution plan
 * @param schema: the schema of this query
 * @param running: bool indicating if the query is running (has been deployed)
 */
class QueryCatalogEntry {
 public:
  QueryCatalogEntry(string queryId, InputQueryPtr inputQuery,
                    NESExecutionPlanPtr nesPlanPtr, bool running)
      :
      queryId(queryId),
      inputQuery(inputQuery),
      nesPlanPtr(nesPlanPtr),
      running(running) {
  }

  string queryId;
  InputQueryPtr inputQuery;
  NESExecutionPlanPtr nesPlanPtr;
  bool running;
};

typedef std::shared_ptr<QueryCatalogEntry> QueryCatalogEntryPtr;

/**
 * @brief catalog class to handle the queries in the system
 * @limitations:
 *  - TODO: does not differ between deployed and started
 *
 */
class QueryCatalog {
 public:
  static QueryCatalog& instance();

  /**
   * @brief registers a CAF query into the NES topology to make it deployable
   * @param queryString a queryString of the query
   * @param optimizationStrategyName the optimization strategy (buttomUp or topDown)
   */
  string register_query(const string &queryString,
                        const string &optimizationStrategyName);

  /**
   * @brief method which is called to unregister an already running query
   * @param queryId the queryId of the query
   * @return true if deleted from running queries, otherwise false
   */
  bool deregister_query(const string &queryId);

  /**
   * @brief method to start a query
   * @param id of the query to start
   * @note this will set the running bool to true in the QueryCatalogEntry
   */
  void startQuery(string queryId);

  /**
   * @brief method to stop a query
   * @param id of the query to stop
   * @note this will set the running bool to false in the QueryCatalogEntry
   */
  void stopQuery(string queryId);

  /**
   * @brief method to test if a query is started
   * @param id of the query to stop
   * @note this will set the running bool to false in the QueryCatalogEntry
   */
  bool testIfQueryStarted(string queryId);

  /**
   * @brief method to get the registered queries
   * @note this contain all queries running/not running
   * @return this will return a COPY of the queries in the catalog
   */
  map<string, QueryCatalogEntryPtr> getRegisteredQueries();

  /**
   * @brief method to get a particular query
   * @param id of the query
   * @return pointer to the catalog entry
   */
  QueryCatalogEntryPtr getQuery(std::string queryID);

  /**
   * @brief method to thest if a query exists
   * @param query id
   * @return bool indicating if query exists (true) or not (false)
   */
  bool testIfQueryExists(std::string queryID);

  /**
   * @brief method to get the running queries
   * @note this contain only running queries
   * @return this will return a COPY of the queries in the catalog that are running
   */
  map<string, QueryCatalogEntryPtr> getRunningQueries();

  /**
   * @brief method to reset the catalog
   */
  void clearQueries();

  /**
   * @brief method to get the content of the query catalog as a string
   * @return entries in the catalog as a string
   */
  std::string printQueries();
 private:

  map<string, QueryCatalogEntryPtr> queries;
};
}

#endif /* INCLUDE_CATALOGS_QUERYCATALOG_HPP_ */


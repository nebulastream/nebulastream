#ifndef INCLUDE_CATALOGS_QUERYCATALOG_HPP_
#define INCLUDE_CATALOGS_QUERYCATALOG_HPP_

#include <unordered_map>
#include <string>
#include <Optimizer/NESExecutionPlan.hpp>
#include <Catalogs/QueryCatalog.hpp>

using namespace std;
namespace NES {

class QueryCatalogEntry {
 public:

  QueryCatalogEntry(string queryId,
      InputQueryPtr inputQuery,
      NESExecutionPlanPtr nesPlanPtr,
      Schema schema,
      bool running):
  queryId(queryId),
  inputQuery(inputQuery),
  nesPlanPtr(nesPlanPtr),
  schema(schema),
  running(running)
  {

  }

  string queryId;
  InputQueryPtr inputQuery;
  NESExecutionPlanPtr nesPlanPtr;
  Schema schema;  //TODO: do we really need this?
  bool running;

};

typedef std::shared_ptr<QueryCatalogEntry> QueryCatalogEntryPtr;


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

  bool startQuery(string queryId);

  bool stopQuery(string queryId);

  map<string, QueryCatalogEntryPtr> getRegisteredQueries();

  QueryCatalogEntryPtr getQuery(std::string queryID);

  bool testIfQueryExists(std::string queryID);
  map<string, QueryCatalogEntryPtr> getRunningQueries();

  void clearQueries();

 private:

  map<string, QueryCatalogEntryPtr> queries;
//  unordered_map<string, tuple<Schema, NESExecutionPlan>> runningQueries;
};
}

#endif /* INCLUDE_CATALOGS_QUERYCATALOG_HPP_ */


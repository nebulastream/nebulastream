#ifndef INCLUDE_CATALOGS_QUERYCATALOG_HPP_
#define INCLUDE_CATALOGS_QUERYCATALOG_HPP_

#include <Catalogs/QueryCatalogEntry.hpp>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace NES {

class StreamCatalog;
typedef std::shared_ptr<StreamCatalog> StreamCatalogPtr;
class GlobalExecutionPlan;
typedef std::shared_ptr<GlobalExecutionPlan> GlobalExecutionPlanPtr;

static std::map<std::string, QueryStatus> stringToQueryStatusMap{
    {"REGISTERED", Registered},
    {"SCHEDULING", Scheduling},
    {"RUNNING", Running},
    {"STOPPED", Stopped},
    {"FAILED", Failed},
};

/**
 * @brief catalog class to handle the queries in the system
 * @limitations:
 *  - TODO: does not differ between deployed and started
 *
 */
class QueryCatalog {
  public:
    QueryCatalog(TopologyManagerPtr topologyManager, StreamCatalogPtr streamCatalog, GlobalExecutionPlanPtr globalExecutionPlan);
    /**
     * @brief registers an RPC query into the NES topology to make it deployable
     * @param queryString a queryString of the query
     * @param optimizationStrategyName the optimization strategy (bottomUp or topDown)
     * @return query id
     */
    string registerQuery(const string& queryString,
                         const string& optimizationStrategyName);

    /**
     * @brief method which is called to unregister an already running query
     * @param queryId the queryId of the query
     * @return true if deleted from running queries, otherwise false
     */
    bool deleteQuery(const string& queryId);

    /**
     * @brief method to change status of a query
     * @param id of the query
     * @param status of the query
     */
    void markQueryAs(string queryId, QueryStatus queryStatus);

    /**
     * @brief method to test if a query is started
     * @param id of the query to stop
     * @note this will set the running bool to false in the QueryCatalogEntry
     */
    bool isQueryRunning(string queryId);

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
     * @brief method to test if a query exists
     * @param query id
     * @return bool indicating if query exists (true) or not (false)
     */
    bool queryExists(std::string queryId);

    /**
     * @brief method to get the queries in a specific state
     * @param requestedStatus : desired query status
     * @return this will return a COPY of the queries in the catalog that are running
     */
    map<string, QueryCatalogEntryPtr> getQueries(QueryStatus requestedStatus);

    /**
     * @brief method to reset the catalog
     */
    void clearQueries();

    /**
     * @brief method to get the content of the query catalog as a string
     * @return entries in the catalog as a string
     */
    std::string printQueries();

    /**
    * @brief Get the queries in the user defined status
    * @param status : query status
    * @return returns map of query Id and query string
    * @throws exception in case of invalid status
    */
    std::map<std::string, std::string> getQueriesWithStatus(std::string status);

    /**
     * @brief Get all queries registered in the system
     * @return map of query ids and query string with query status
     */
    std::map<std::string, std::string> getAllRegisteredQueries();

  private:
    TopologyManagerPtr topologyManager;
    StreamCatalogPtr streamCatalog;
    GlobalExecutionPlanPtr globalExecutionPlan;
    std::map<string, QueryCatalogEntryPtr> queries;
    std::mutex insertDeleteQuery;
};

typedef std::shared_ptr<QueryCatalog> QueryCatalogPtr;
}// namespace NES

#endif /* INCLUDE_CATALOGS_QUERYCATALOG_HPP_ */

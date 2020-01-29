#ifndef INCLUDE_CATALOGS_QUERYCATALOG_HPP_
#define INCLUDE_CATALOGS_QUERYCATALOG_HPP_

#include <unordered_map>
#include <string>
#include <Optimizer/NESExecutionPlan.hpp>
#include <Catalogs/QueryCatalog.hpp>

using namespace std;
namespace NES {

/**
 * @brief Represents various states the user query goes through.
 *
 * Registered : Query is registered to be scheduled to the worker nodes
 * Scheduling: Coordinator node is transmitting the execution pipelines to worker nodes
 * Running: Query is now running successfully
 * Stopped: Query was explicitly stopped by system
 * Failed: Query failed because of some reason
 *
 */
enum QueryStatus { Registered, Scheduling, Running, Stopped, Failed };

static std::map<std::string, QueryStatus> StringToQueryStatus{
    {"REGISTERED", Registered},
    {"SCHEDULING", Scheduling},
    {"RUNNING", Running},
    {"STOPPED", Stopped},
    {"FAILED", Failed},
};

/**
 * @brief class to handle the entry in the query catalog
 * @param queryId: id of the query (is also the key in the queries map)
 * @param queryString: string representation of the input query
 * @param inputQueryPtr: a pointer to the input query
 * @param nesPlanPtr: a pointer to the generated nes execution plan
 * @param schema: the schema of this query
 * @param running: bool indicating if the query is running (has been deployed)
 */
class QueryCatalogEntry {
  public:
    QueryCatalogEntry(string queryId, string queryString, InputQueryPtr inputQueryPtr,
                      NESExecutionPlanPtr nesPlanPtr, QueryStatus queryStatus)
        :
        queryId(queryId),
        queryString(queryString),
        inputQueryPtr(inputQueryPtr),
        nesPlanPtr(nesPlanPtr),
        queryStatus(queryStatus) {
    }

    string queryId;
    string queryString;
    InputQueryPtr inputQueryPtr;
    NESExecutionPlanPtr nesPlanPtr;
    QueryStatus queryStatus;
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
    string registerQuery(const string& queryString,
                         const string& optimizationStrategyName);

    /**
     * @brief method which is called to unregister an already running query
     * @param queryId the queryId of the query
     * @return true if deleted from running queries, otherwise false
     */
    bool deleteQuery(const string& queryId);

    /**
     * @brief method to mark a query running
     * @param id of the query to start
     * @note this will set the running bool to true in the QueryCatalogEntry
     */
    void markQueryAsRunning(string queryId);

    /**
     * @brief method to mark a query stop
     * @param id of the query to stop
     * @note this will set the running bool to false in the QueryCatalogEntry
     */
    void markQueryAsStopped(string queryId);

    /**
     * @brief method to mark a query failed
     * @param id of the query to stop
     * @note this will set the running bool to false in the QueryCatalogEntry
     */
    void markQueryAsFailed(string queryId);

    /**
     * @brief method to mark a query as scheduling
     * @param id of the query to stop
     * @note this will set the running bool to false in the QueryCatalogEntry
     */
    void markQueryAsScheduling(string queryId);

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
     * @brief method to thest if a query exists
     * @param query id
     * @return bool indicating if query exists (true) or not (false)
     */
    bool isQueryExists(std::string queryId);

    /**
     * @brief method to get the queries in a specific state
     * @param queryStatus : desired query status
     * @return this will return a COPY of the queries in the catalog that are running
     */
    map<string, QueryCatalogEntryPtr> getQueries(QueryStatus queryStatus);

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


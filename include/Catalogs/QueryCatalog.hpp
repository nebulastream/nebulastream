#ifndef INCLUDE_CATALOGS_QUERYCATALOG_HPP_
#define INCLUDE_CATALOGS_QUERYCATALOG_HPP_

#include <Catalogs/QueryCatalogEntry.hpp>
#include <Plans/Query/QueryId.hpp>
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <vector>

namespace NES {

/**
 * @brief catalog class to handle the queries in the system
 * @limitations:
 *  - TODO: does not differ between deployed and started
 *
 */
class QueryCatalog {
  public:
    QueryCatalog();
    ~QueryCatalog();

    /**
     * @brief registers a new query into the NES Query catalog and add it to the scheduling queue for later execution.
     * @param queryString: a user query in string form
     * @param queryPlan: a user query plan to be executed
     * @param optimizationStrategyName: the optimization strategy (bottomUp or topDown)
     * @return query catalog entry or nullptr
     */
    QueryCatalogEntryPtr addNewQueryRequest(const std::string& queryString, const QueryPlanPtr queryPlan, const std::string& optimizationStrategyName);

    /**
     * @brief register a request for stopping a query and add it to the scheduling queue.
     * @param queryId: id of the user query.
     * @return query catalog entry or nullptr
     */
    QueryCatalogEntryPtr addQueryStopRequest(QueryId queryId);

    /**
     * @brief method to change status of a query
     * @param id of the query
     * @param status of the query
     */
    void markQueryAs(QueryId queryId, QueryStatus newStatus);

    /**
     * @brief method to test if a query is started
     * @param id of the query to stop
     * @note this will set the running bool to false in the QueryCatalogEntry
     */
    bool isQueryRunning(QueryId queryId);

    /**
     * @brief method to get the registered queries
     * @note this contain all queries running/not running
     * @return this will return a COPY of the queries in the catalog
     */
    std::map<uint64_t, QueryCatalogEntryPtr> getAllQueryCatalogEntries();

    /**
     * @brief method to get a particular query
     * @param id of the query
     * @return pointer to the catalog entry
     */
    QueryCatalogEntryPtr getQueryCatalogEntry(QueryId queryId);

    /**
     * @brief method to test if a query exists
     * @param query id
     * @return bool indicating if query exists (true) or not (false)
     */
    bool queryExists(QueryId queryId);

    /**
     * @brief method to get the queries in a specific state
     * @param requestedStatus : desired query status
     * @return this will return a COPY of the queries in the catalog that are running
     */
    std::map<uint64_t, QueryCatalogEntryPtr> getQueries(QueryStatus requestedStatus);

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
    std::map<uint64_t, std::string> getQueriesWithStatus(std::string status);

    /**
     * @brief Get all queries registered in the system
     * @return map of query ids and query string with query status
     */
    std::map<uint64_t, std::string> getAllQueries();

  private:
    // TODO this is a temp fix, please look at issue
    // TODO 890 to have a proper fix
    std::recursive_mutex catalogMutex;
    std::map<uint64_t, QueryCatalogEntryPtr> queries;
};

typedef std::shared_ptr<QueryCatalog> QueryCatalogPtr;
}// namespace NES

#endif /* INCLUDE_CATALOGS_QUERYCATALOG_HPP_ */

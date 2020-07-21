#ifndef INCLUDE_CATALOGS_QUERYCATALOG_HPP_
#define INCLUDE_CATALOGS_QUERYCATALOG_HPP_

#include <Catalogs/QueryCatalogEntry.hpp>
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

    /**
     * @brief registers a new query into the NES Query catalog and add it to the scheduling queue for later execution.
     * @param queryString: a user query in string form
     * @param queryPlan: a user query plan to be executed
     * @param optimizationStrategyName: the optimization strategy (bottomUp or topDown)
     * @return true if registration successful else false
     */
    bool registerAndQueueAddRequest(const std::string& queryString, const QueryPlanPtr queryPlan, const std::string& optimizationStrategyName);

    /**
     * @brief register a request for stopping a query and add it to the scheduling queue.
     * @param queryId: id of the user query.
     * @return true if successful
     */
    bool queueStopRequest(std::string queryId);

    /**
     * @brief Get a batch of query catalog entries to be scheduled.
     * Note: This method returns only a copy of the
     * @return a vector of query catalog entry to schedule
     */
    std::vector<QueryCatalogEntry> getQueriesToSchedule();

    /**
     * @brief method to change status of a query
     * @param id of the query
     * @param status of the query
     */
    void markQueryAs(std::string queryId, QueryStatus newStatus);

    /**
     * @brief method to test if a query is started
     * @param id of the query to stop
     * @note this will set the running bool to false in the QueryCatalogEntry
     */
    bool isQueryRunning(std::string queryId);

    /**
     * @brief method to get the registered queries
     * @note this contain all queries running/not running
     * @return this will return a COPY of the queries in the catalog
     */
    std::map<std::string, QueryCatalogEntryPtr> getAllQueryCatalogEntries();

    /**
     * @brief method to get a particular query
     * @param id of the query
     * @return pointer to the catalog entry
     */
    QueryCatalogEntryPtr getQueryCatalogEntry(std::string queryId);

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
    std::map<std::string, QueryCatalogEntryPtr> getQueries(QueryStatus requestedStatus);

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
    std::map<std::string, std::string> getAllQueries();

    /**
     * @brief Check if there are new request available
     * @return true if there are new requests
     */
    bool isNewRequestAvailable() const;

    /**
     * @brief Change status of new request availability
     * @param newRequestAvailable: bool indicating if the request is available
     */
    void setNewRequestAvailable(bool newRequestAvailable);

    /**
     * @brief This method will force trigger the availabilityTrigger conditional variable in order to
     * interrupt the wait.
     */
    void insertPoisonPill();

  private:
    std::mutex queryStatus;
    std::mutex queryRequest;
    std::condition_variable availabilityTrigger;
    bool newRequestAvailable;
    std::map<std::string, QueryCatalogEntryPtr> queries;
    std::deque<QueryCatalogEntryPtr> schedulingQueue;
    int64_t batchSize = 1;
};

typedef std::shared_ptr<QueryCatalog> QueryCatalogPtr;
}// namespace NES

#endif /* INCLUDE_CATALOGS_QUERYCATALOG_HPP_ */

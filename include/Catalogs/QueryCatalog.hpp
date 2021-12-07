/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#ifndef NES_INCLUDE_CATALOGS_QUERY_CATALOG_HPP_
#define NES_INCLUDE_CATALOGS_QUERY_CATALOG_HPP_

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
    QueryCatalog() = default;

    /**
     * @brief registers a new query into the NES Query catalog and add it to the scheduling queue for later execution.
     * @param queryString: a user query in string form
     * @param queryPlan: a user query plan to be executed
     * @param optimizationStrategyName: the optimization strategy (bottomUp or topDown)
     * @return query catalog entry or nullptr
     */
    QueryCatalogEntryPtr
    addNewQuery(const std::string& queryString, QueryPlanPtr const& queryPlan, const std::string& optimizationStrategyName);

    QueryCatalogEntryPtr recordInvalidQuery(std::string const& queryString,
                                            QueryId queryId,
                                            QueryPlanPtr const& queryPlan,
                                            std::string const& placementStrategyName);
    /**
     * @brief Set the executed query plan for the query id
     * @param queryId : the original query id
     * @param executedQueryPlan : the query plan executed by NES
     */
    void setExecutedQueryPlanForQuery(QueryId queryId, QueryPlanPtr executedQueryPlan);

    void setQueryFailureReason(QueryId queryId, const std::string& failureReason);

    /**
     * @brief register a request for stopping a query and add it to the scheduling queue.
     * @param queryId: id of the user query.
     * @return query catalog entry or nullptr
     */
    QueryCatalogEntryPtr stopQuery(QueryId queryId);

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

using QueryCatalogPtr = std::shared_ptr<QueryCatalog>;
}// namespace NES

#endif// NES_INCLUDE_CATALOGS_QUERY_CATALOG_HPP_

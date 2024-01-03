/*
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

#ifndef NES_CATALOGS_INCLUDE_CATALOGS_QUERY_QUERYCATALOG_HPP_
#define NES_CATALOGS_INCLUDE_CATALOGS_QUERY_QUERYCATALOG_HPP_

#include <Identifiers.hpp>
#include <Util/Placement/PlacementStrategy.hpp>
#include <Util/QueryState.hpp>
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <vector>

namespace NES {

class QueryPlan;
using QueryPlanPtr = std::shared_ptr<QueryPlan>;

namespace Catalogs::Query {

class QueryCatalogEntry;
using QueryCatalogEntryPtr = std::shared_ptr<QueryCatalogEntry>;

/**
 * @brief catalog class to handle the queryIdAndCatalogEntryMapping in the system
 * @note: This class is not thread safe. Please use QueryCatalogService to access this object.
 */
class QueryCatalog {
  public:
    QueryCatalog() = default;

    /**
     * @brief registers a new query into the NES Query catalog and add it to the scheduling queue for later execution.
     * @param queryString: a user query in string form
     * @param queryPlan: a user query plan to be executed
     * @param placementStrategyName: the placement strategy (e.g. bottomUp, topDown, etc)
     * @return query catalog entry or nullptr
     */
    QueryCatalogEntryPtr createNewEntry(const std::string& queryString,
                                        const QueryPlanPtr& queryPlan,
                                        const Optimizer::PlacementStrategy placementStrategyName);

    /**
     * @brief method to get the registered queryIdAndCatalogEntryMapping
     * @note this contain all queryIdAndCatalogEntryMapping running/not running
     * @return this will return a COPY of the queryIdAndCatalogEntryMapping in the catalog
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
     * @brief method to get the queryIdAndCatalogEntryMapping in a specific state
     * @param requestedStatus : desired query status
     * @return this will return a COPY of the queryIdAndCatalogEntryMapping in the catalog that are running
     */
    std::map<uint64_t, QueryCatalogEntryPtr> getQueryCatalogEntries(QueryState requestedStatus);

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
    * @brief Get the queryIdAndCatalogEntryMapping in the user defined status
    * @param status : query status
    * @return returns map of query Id and query string
    * @throws exception in case of invalid status
    */
    std::map<uint64_t, std::string> getQueriesWithStatus(QueryState status);

    /**
     * @brief Get all queryIdAndCatalogEntryMapping registered in the system
     * @return map of query ids and query string with query status
     */
    std::map<uint64_t, std::string> getAllQueries();

    /**
     * map shared query plan id to the query catalog entry
     * @param sharedQueryId : the shared query id
     * @param queryCatalogEntry : the query catalog entry
     */
    void mapSharedQueryPlanId(SharedQueryId sharedQueryId, QueryCatalogEntryPtr queryCatalogEntry);

    /**
     * @brief Get all query catalog entries mapped to the shared query plan
     * @param sharedQueryId : the shared query plan id
     * @return vector of query catalog entries
     */
    std::vector<QueryCatalogEntryPtr> getQueryCatalogEntriesForSharedQueryId(SharedQueryId sharedQueryId);

    /**
     * map shared query plan id to the input query id
     * @param sharedQueryId : the shared query id
     * @param queryId : the query id
     */
    void removeSharedQueryPlanIdMappings(SharedQueryId sharedQueryId);

  private:
    std::map<uint64_t, QueryCatalogEntryPtr> queryIdAndCatalogEntryMapping;
    std::map<SharedQueryId, std::vector<QueryCatalogEntryPtr>> sharedQueryIdAndCatalogEntryMapping;
};

using QueryCatalogPtr = std::shared_ptr<QueryCatalog>;
}// namespace Catalogs::Query
}// namespace NES
#endif // NES_CATALOGS_INCLUDE_CATALOGS_QUERY_QUERYCATALOG_HPP_

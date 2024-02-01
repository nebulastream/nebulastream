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
#include <folly/Synchronized.h>
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

class SharedQueryCatalogEntry;
using SharedQueryCatalogEntryPtr = std::shared_ptr<SharedQueryCatalogEntry>;

/**
 * @brief catalog class to handle the queryIdAndCatalogEntryMapping in the system
 * @note: This class is not thread safe. Please use QueryCatalogService to access this object.
 */
class QueryCatalog {
  public:
    QueryCatalog() = default;

    /**
     * @brief registers a new query into the NES Query catalog.
     * @param queryString: a user query in string form
     * @param queryPlan: a user query plan to be executed
     * @param placementStrategyName: the placement strategy (e.g. bottomUp, topDown, etc)
     * @param queryState: the state of the query
     */
    void createQueryCatalogEntry(const std::string& queryString,
                                 const QueryPlanPtr& queryPlan,
                                 const Optimizer::PlacementStrategy placementStrategyName,
                                 QueryState queryState);

    /**
     * @brief registers a new shared query into the Query catalog
     * @param sharedQueryId: a user query in string form
     * @param queryIds: query ids contained in the shared query plan
     * @param queryStatus: the status of the shared query
     */
    void createSharedQueryCatalogEntry(SharedQueryId sharedQueryId, std::vector<QueryId> queryIds, QueryState queryStatus);

    /**
     * @brief Update query entry with new status
     * @param queryId : query id
     * @param queryStatus : new status
     * @param metaInformation : additional meta information
     * @return true if updated successfully
     */
    bool updateQueryStatus(QueryId queryId, QueryState queryStatus, const std::string& metaInformation);

    /**
     * @brief Update query entry with new status
     * @param sharedQueryId : shared query id
     * @param queryState : new status
     * @param metaInformation : additional meta information
     * @return true if updated successfully
     */
    bool updateSharedQueryStatus(SharedQueryId sharedQueryId, QueryState queryState, const std::string& metaInformation);

    /**
     * @brief check and mark the shared query for soft stop
     * @param sharedQueryId: the query which is being soft stopped
     * @return true if successful else false
     */
    bool checkAndMarkSharedQueryForSoftStop(SharedQueryId sharedQueryId,
                                            DecomposedQueryPlanId decomposedQueryPlanId,
                                            OperatorId operatorId);

    /**
     * @brief check and mark the query for hard stop
     * @param queryId: the query which need to be stopped
     * @return true if successful else false
     */
    bool checkAndMarkQueryForHardStop(QueryId queryId);

    /**
     * @brief Check and mark shared query for failure
     * @param sharedQueryId
     * @param querySubPlanId
     */
    void checkAndMarkSharedQueryForFailure(SharedQueryId sharedQueryId, DecomposedQueryPlanId querySubPlanId);

    /**
     * Add update query plans to the query catalog
     * @param queryId : the query id
     * @param step : step that produced the updated plan
     * @param updatedQueryPlan : the updated query plan
     */
    void addUpdatedQueryPlan(QueryId queryId, std::string step, QueryPlanPtr updatedQueryPlan);

    /**
     * Add sub query meta data to the query
     * @param sharedQueryId : query id to which sub query metadata to add
     * @param decomposedQueryPlanId : the sub query plan id
     * @param workerId : the topology node where the sub query plan is running
     * @param querySubPlanStatus : the state of the sub query
     */
    void addDecomposedQueryMetaData(QueryId sharedQueryId,
                                    DecomposedQueryPlanId decomposedQueryPlanId,
                                    WorkerId workerId,
                                    QueryState querySubPlanStatus);

    /**
     * Update decomposed query plan status
     * @param sharedQueryId: the query id to which sub plan is added
     * @param decomposedQueryPlanId: the query sub plan id
     * @param decomposedQueryPlanVersion: the decomposed query plan version
     * @param queryState : the new decomposed query plan status
     */
    bool updateDecomposedQueryPlanStatus(SharedQueryId sharedQueryId,
                                         DecomposedQueryPlanId decomposedQueryPlanId,
                                         DecomposedQueryPlanVersion decomposedQueryPlanVersion,
                                         QueryState queryState);

  private:
    /**
     * @brief method to get a particular query
     * @param id of the query
     * @return pointer to the catalog entry
     */
    QueryCatalogEntryPtr getQueryCatalogEntry(QueryId queryId);

    /**
     * @brief Check if a query with given id registered in the catalog
     * @param query the query id
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
     * @brief method to get the registered queryIdAndCatalogEntryMapping
     * @note this contain all queryIdAndCatalogEntryMapping running/not running
     * @return this will return a COPY of the queryIdAndCatalogEntryMapping in the catalog
     */
    std::map<uint64_t, QueryCatalogEntryPtr> getAllQueryCatalogEntries();

    /**
     * @brief method to reset the catalog
     */
    void resetCatalog();

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
    folly::Synchronized<std::map<QueryId, QueryCatalogEntryPtr>> queryCatalogEntryMapping;
    folly::Synchronized<std::map<SharedQueryId, SharedQueryCatalogEntryPtr>> sharedQueryCatalogEntryMapping;
};

using QueryCatalogPtr = std::shared_ptr<QueryCatalog>;
}// namespace Catalogs::Query
}// namespace NES
#endif// NES_CATALOGS_INCLUDE_CATALOGS_QUERY_QUERYCATALOG_HPP_

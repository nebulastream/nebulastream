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

#ifndef NES_CATALOGS_INCLUDE_CATALOGS_QUERY_QUERYCATALOGSERVICE_HPP_
#define NES_CATALOGS_INCLUDE_CATALOGS_QUERY_QUERYCATALOGSERVICE_HPP_

#include <Identifiers.hpp>
#include <Util/PlacementStrategy.hpp>
#include <Util/QueryState.hpp>
#include <map>
#include <memory>
#include <mutex>
#include <vector>

namespace NES {

class QueryCatalogService;
using QueryCatalogServicePtr = std::shared_ptr<QueryCatalogService>;

class QueryPlan;
using QueryPlanPtr = std::shared_ptr<QueryPlan>;

namespace Catalogs::Query {

class QueryCatalogEntry;
using QueryCatalogEntryPtr = std::shared_ptr<QueryCatalogEntry>;

class QueryCatalog;
using QueryCatalogPtr = std::shared_ptr<QueryCatalog>;

}// namespace Catalogs::Query

/**
 * This class is responsible for interacting with query catalog to either fetch status of a query or to update it.
 */
class QueryCatalogService {

  public:
    explicit QueryCatalogService(Catalogs::Query::QueryCatalogPtr queryCatalog);

    /**
     * @brief registers a new query into the NES Query catalog and add it to the scheduling queue for later execution.
     * @param queryString: a user query in string form
     * @param queryPlan: a user query plan to be executed
     * @param placementStrategyName: the placement strategy (e.g. bottomUp, topDown, etc)
     * @return query catalog entry or nullptr
     */
    Catalogs::Query::QueryCatalogEntryPtr createNewEntry(const std::string& queryString,
                                                         QueryPlanPtr const& queryPlan,
                                                         const Optimizer::PlacementStrategy placementStrategyName);

    /**
     * Add sub query meta data to the query
     * @param queryId : query id to which sub query metadata to add
     * @param querySubPlanId : the sub query plan id
     * @param workerId : the topology node where the sub query plan is running
     * @param querySubPlanStatus : the state of the sub query
     */
    void addSubQueryMetaData(QueryId queryId, QuerySubPlanId querySubPlanId, uint64_t workerId, QueryState querySubPlanStatus);

    /**
     * Reset all sub query plans added to the query
     * @param queryId : the query id
     */
    void resetSubQueryMetaData(QueryId queryId);

    /**
     * Update query sub plan status
     * @param sharedQueryId : the query id to which sub plan is added
     * @param querySubPlanId : the query sub plan id
     * @param querySubPlanStatus : the new sub query status
     */
    bool updateQuerySubPlanStatus(SharedQueryId sharedQueryId, QuerySubPlanId querySubPlanId, QueryState querySubPlanStatus);

    /**
     * Get the entry from the query catalog for the input query id
     * @param queryId: the query id
     * @return pointer to the query catalog entry
     */
    Catalogs::Query::QueryCatalogEntryPtr getEntryForQuery(QueryId queryId);

    /**
     * Get a map of query id and query string that are in input query status
     * @param queryStatus : the status to check
     * @return map containing query id and query string
     */
    std::map<uint64_t, std::string> getAllQueriesInStatus(std::string queryStatus);

    /**
     * Get a map of query id and query catalog entry in input query status
     * @param queryStatus : the status to check
     * @return map containing query id and query catalog entry
     */
    std::map<uint64_t, Catalogs::Query::QueryCatalogEntryPtr> getAllEntriesInStatus(std::string queryStatus);

    /**
     * Get all query catalog entries
     * @return map containing query id and query catalog entry
     */
    std::map<uint64_t, Catalogs::Query::QueryCatalogEntryPtr> getAllQueryCatalogEntries();

    /**
     * Update query entry with new status
     * @param queryId : query id
     * @param queryStatus : new status
     * @param metaInformation : additional meta information
     * @return true if updated successfully
     */
    bool updateQueryStatus(QueryId queryId, QueryState queryStatus, const std::string& metaInformation);

    /**
     * check and mark the query for soft stop
     * @param sharedQueryId: the query which need to be stopped
     * @return true if successful else false
     */
    bool checkAndMarkForSoftStop(SharedQueryId sharedQueryId, QuerySubPlanId subPlanId, OperatorId operatorId);

    /**
     * check and mark the query for hard stop
     * @param queryId: the query which need to be stopped
     * @return true if successful else false
     */
    bool checkAndMarkForHardStop(QueryId queryId);

    /**
     * Add update query plans to the query catalog
     * @param queryId : the query id
     * @param step : step that produced the updated plan
     * @param updatedQueryPlan : the updated query plan
     */
    void addUpdatedQueryPlan(QueryId queryId, std::string step, QueryPlanPtr updatedQueryPlan);

    /**
     * Mapping shard query plan id to the query id
     * @param sharedQueryId : the shared query plan id
     * @param queryId : the query id
     */
    void mapSharedQueryPlanId(SharedQueryId sharedQueryId, QueryId queryId);

    /**
     * Remove mapping of the shared query plan
     * @param sharedQueryId : the shared query plan id
     */
    void removeSharedQueryPlanMapping(SharedQueryId sharedQueryId);

    /**
     * Fetch all query ids that belong to the shared query plan
     * @param sharedQueryId : the id of the shared query plan
     * @return collection of ids of queries merged in shared query plan
     */
    std::vector<QueryId> getQueryIdsForSharedQueryId(SharedQueryId sharedQueryId);

    /**
     * Clear the query catalog
     */
    void clearQueries();

    /**
     * Check and mark all query belonging to the shared query plan for failure
     * @param sharedQueryId : id of the shared query plan
     * @param querySubPlanId : id of the sub query plan that failed
     * @throws QueryNotFoundException if no query with the supplied id exists
     * @throws InvalidQueryStateException if the query is in status MARKED_FOR_FAILURE, FAILED of STOPPED
     */
    void checkAndMarkForFailure(SharedQueryId sharedQueryId, QuerySubPlanId querySubPlanId);

    /**
     * Indicate that a query sub plan will be stopped and the operators will be deployed somewhere else. This has to
     * be called before entering the query deployment phase for a shared query that is to be redeployed.
     * @param sharedQueryId: the query id
     * @param querySubPlanId : query sub plan id
     * @param querySubPlanStatus : the new status
     * @return true if successful else false
     */
    bool checkAndMarkForMigration(SharedQueryId sharedQueryId, QuerySubPlanId querySubPlanId, QueryState querySubPlanStatus);
  private:
    /**
     * Handle soft stop for sub query plans
     * @param sharedQueryId: the query id
     * @param querySubPlanId : query sub plan id
     * @param querySubPlanStatus : the new status
     * @return true if successful else false
     */
    bool handleSoftStop(SharedQueryId sharedQueryId, QuerySubPlanId querySubPlanId, QueryState querySubPlanStatus);


    Catalogs::Query::QueryCatalogPtr queryCatalog;
    std::recursive_mutex serviceMutex;
};
}// namespace NES

#endif // NES_CATALOGS_INCLUDE_CATALOGS_QUERY_QUERYCATALOGSERVICE_HPP_

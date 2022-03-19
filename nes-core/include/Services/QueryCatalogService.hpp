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

#ifndef NES_QUERYCATALOGSERVICE_HPP
#define NES_QUERYCATALOGSERVICE_HPP

#include <Plans/Query/QueryId.hpp>
#include <Plans/Query/QuerySubPlanId.hpp>
#include <Util/QueryStatus.hpp>
#include <memory>
#include <mutex>

namespace NES {

class QueryCatalogEntry;
using QueryCatalogEntryPtr = std::shared_ptr<QueryCatalogEntry>;

class QueryCatalogService;
using QueryCatalogServicePtr = std::shared_ptr<QueryCatalogService>;

class QueryCatalog;
using QueryCatalogPtr = std::shared_ptr<QueryCatalog>;

class QueryPlan;
using QueryPlanPtr = std::shared_ptr<QueryPlan>;

/**
 * This class is responsible for interacting with query catalog to either fetch status of a query or to update it.
 */
class QueryCatalogService {

  public:
    explicit QueryCatalogService(QueryCatalogPtr queryCatalog);

    /**
     * @brief registers a new query into the NES Query catalog and add it to the scheduling queue for later execution.
     * @param queryString: a user query in string form
     * @param queryPlan: a user query plan to be executed
     * @param placementStrategyName: the placement strategy (e.g. bottomUp, topDown, etc)
     * @return query catalog entry or nullptr
     */
    QueryCatalogEntryPtr
    createNewEntry(const std::string& queryString, QueryPlanPtr const& queryPlan, std::string const& placementStrategyName);

    /**
     *
     * @param queryId
     * @param querySubPlanId
     * @param workerId
     */
    void addSubQuery(QueryId queryId, QuerySubPlanId querySubPlanId, uint64_t workerId);

    /**
     *
     * @param queryId
     * @param querySubPlanId
     * @param subQueryStatus
     */
    void updateQuerySubPlanStatus(QueryId queryId, QuerySubPlanId querySubPlanId, QueryStatus::Value subQueryStatus);

    /**
     *
     * @param queryId
     * @return
     */
    QueryCatalogEntryPtr getEntryForQuery(QueryId queryId);

    /**
     *
     * @param queryId
     * @return
     */
    std::map<uint64_t, std::string> getAllEntriesInStatus(std::string queryStatus);

    /**
     *
     * @return
     */
    std::map<uint64_t, QueryCatalogEntryPtr> getAllQueryCatalogEntries();

    /**
     *
     * @param queryId
     * @param queryStatus
     * @return
     */
    bool updateQueryStatus(QueryId queryId, QueryStatus::Value queryStatus, const std::string& metaInformation);

    /**
     *
     * @param queryId
     * @return
     */
    bool checkAndMarkForSoftStop(QueryId queryId);

    /**
     *
     * @param queryId
     * @return
     */
    bool checkAndMarkForHardStop(QueryId queryId);

    /**
     *
     * @param queryId
     * @param querySubPlanId
     * @param triggered
     * @return
     */
    bool registerSoftStopTriggered(QueryId queryId, QuerySubPlanId querySubPlanId, bool triggered);

    /**
     *
     * @param queryId
     * @param querySubPlanId
     * @param completed
     * @return
     */
    bool registerSoftStopCompleted(QueryId queryId, QuerySubPlanId querySubPlanId, bool completed);

    void addUpdatedQueryPlan(QueryId queryId, std::string step, QueryPlanPtr updatedQueryPlan);

    void clearQueries();

  private:
    bool handleSoftStopCompletion(QueryId queryId, QuerySubPlanId querySubPlanId, QueryStatus::Value subQueryStatus);

    QueryCatalogPtr queryCatalog;
    std::recursive_mutex serviceMutex;
};
}// namespace NES

#endif//NES_QUERYCATALOGSERVICE_HPP

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

#ifndef NES_OPTIMIZER_INCLUDE_PLANS_GLOBAL_QUERY_GLOBALQUERYPLAN_HPP_
#define NES_OPTIMIZER_INCLUDE_PLANS_GLOBAL_QUERY_GLOBALQUERYPLAN_HPP_

#include <Identifiers/Identifiers.hpp>
#include <Util/Placement/PlacementStrategy.hpp>
#include <Util/RequestType.hpp>
#include <algorithm>
#include <iostream>
#include <map>
#include <memory>
#include <vector>

namespace NES {

class Operator;
using OperatorPtr = std::shared_ptr<Operator>;

class QueryPlan;
using QueryPlanPtr = std::shared_ptr<QueryPlan>;

class GlobalQueryPlan;
using GlobalQueryPlanPtr = std::shared_ptr<GlobalQueryPlan>;

class Topology;
using TopologyPtr = std::shared_ptr<Topology>;

class SourceLogicalOperator;
using SourceLogicalOperatorPtr = std::shared_ptr<SourceLogicalOperator>;

class SinkLogicalOperator;
using SinkLogicalOperatorPtr = std::shared_ptr<SinkLogicalOperator>;

class SharedQueryPlan;
using SharedQueryPlanPtr = std::shared_ptr<SharedQueryPlan>;

/**
 * @brief This class is responsible for storing all currently running and to be deployed QueryPlans in the NES system.
 * The QueryPlans included in the GlobalQueryPlan can be fused together and therefore each operator in GQP contains
 * information about the set of queryIdAndCatalogEntryMapping it belongs to. The QueryPlans are bound together by a dummy logical root operator.
 */
class GlobalQueryPlan {
  public:
    static GlobalQueryPlanPtr create();

    /**
     * @brief Add query plan to the collection of query plans to be merged
     * @param queryPlan : new query plan to be merged.
     * @throws: QueryNotFoundException if the given query plan's id was invalid; GlobalQueryPlanUpdateException if the given
     * query plan was already added
     */
    void addQueryPlan(const QueryPlanPtr& queryPlan);

    /**
     * @brief Create a new shared query plan using the input query plan
     * @param queryPlan : query plan to construct shared query plan.
     * @return: true if successful else false
     */
    bool createNewSharedQueryPlan(const QueryPlanPtr& queryPlan);

    /**
     * @brief remove the operators belonging to the query with input query Id from the global query plan
     * @param queryId: the id of the query whose operators need to be removed
     * @param requestType: request type for query removal
     * todo: #3821 create specific exception for this case
     * @throws RuntimeException: "GlobalQueryPlan: Unable to remove query with id {queryId} from shared query plan with id {sharedQueryId}
     */
    void removeQuery(QueryId queryId, RequestType requestType);

    /**
     * @brief This method will remove all empty shared query plans that are deployed
     */
    void removeFailedOrStoppedSharedQueryPlans();

    /**
     * @brief This method will remove the shared query plan if all involved queries are either stopped or failed
     * @param sharedQueryPlanId id of the shared query plan to be removed
     */
    void removeSharedQueryPlan(SharedQueryId sharedQueryPlanId);

    /**
     * @brief Get the all the Query Meta Data to be deployed
     * @return vector of global query meta data to be deployed
     */
    std::vector<SharedQueryPlanPtr> getSharedQueryPlansToDeploy();

    /**
     * @brief Get all shared query plans in the global query plan
     * @return vector of shared query plans
     */
    std::vector<SharedQueryPlanPtr> getAllSharedQueryPlans();

    /**
     * @brief Get the global query id for the query
     * @param queryId: the original query id
     * @return the corresponding global query id
     */
    SharedQueryId getSharedQueryId(QueryId queryId);

    /**
     * @brief Get all query ids associated with the shared query plan id
     * @param sharedQueryPlanId : the id of the shared query plan
     * @return vector of query ids associated to the input shared query plan id
     */
    std::vector<QueryId> getQueryIds(SharedQueryId sharedQueryPlanId);

    /**
     * @brief Get the shared query metadata information for given shared query id
     * @param sharedQueryId : the shared query id
     * @return SharedQueryPlan or nullptr
     */
    SharedQueryPlanPtr getSharedQueryPlan(SharedQueryId sharedQueryId);

    /**
     * @brief Update the global query meta data information
     * @param sharedQueryPlan: the global query metadata to be updated
     * @return true if successful
     */
    bool updateSharedQueryPlan(const SharedQueryPlanPtr& sharedQueryPlan);

    /**
     * Get query plans to add in the Global query plan
     * @return vector of query plans to add
     */
    const std::vector<QueryPlanPtr>& getQueryPlansToAdd() const;

    /**
     * Clear all query plans that need to be added to the global query plan
     * @return true if successfully cleared else false
     */
    bool clearQueryPlansToAdd();

    /**
     * Fetch the Shared query plan consuming the sources and having the same placement strategy with the input source names and the placement strategy
     * @param sourceNames: the concatenated names of the logical sources
     * @param placementStrategy the placement strategy for the shared query plan
     * @return pointer to the Shared Query Plan or nullptr
     */
    std::vector<SharedQueryPlanPtr>
    getSharedQueryPlansConsumingSourcesAndPlacementStrategy(const std::string& sourceNames,
                                                            NES::Optimizer::PlacementStrategy placementStrategy);

  private:
    GlobalQueryPlan();

    std::map<std::string, std::vector<SharedQueryPlanPtr>> sourceNamesAndPlacementStrategyToSharedQueryPlanMap;
    std::vector<QueryPlanPtr> queryPlansToAdd;
    std::map<QueryId, SharedQueryId> queryIdToSharedQueryIdMap;
    std::map<SharedQueryId, SharedQueryPlanPtr> sharedQueryIdToPlanMap;
};
}// namespace NES
#endif // NES_OPTIMIZER_INCLUDE_PLANS_GLOBAL_QUERY_GLOBALQUERYPLAN_HPP_

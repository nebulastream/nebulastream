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

#ifndef NES_PLACEMENTREMOVALSTRATEGY_HPP
#define NES_PLACEMENTREMOVALSTRATEGY_HPP

#include <Configurations/Enums/PlacementAmendmentMode.hpp>
#include <Identifiers.hpp>
#include <Util/CopiedPinnedOperators.hpp>
#include <folly/Synchronized.h>
#include <set>

namespace NES {

class ExecutionNode;
using ExecutionNodePtr = std::shared_ptr<ExecutionNode>;

class GlobalExecutionPlan;
using GlobalExecutionPlanPtr = std::shared_ptr<GlobalExecutionPlan>;

class QueryPlan;
using QueryPlanPtr = std::shared_ptr<QueryPlan>;

class LogicalOperatorNode;
using LogicalOperatorNodePtr = std::shared_ptr<LogicalOperatorNode>;

class TopologyNode;
using TopologyNodePtr = std::shared_ptr<TopologyNode>;

class Topology;
using TopologyPtr = std::shared_ptr<Topology>;

using TopologyNodeWLock = std::shared_ptr<folly::Synchronized<TopologyNodePtr>::WLockedPtr>;

class PathFinder;
using PathFinderPtr = std::shared_ptr<PathFinder>;

namespace Optimizer {
class TypeInferencePhase;
using TypeInferencePhasePtr = std::shared_ptr<TypeInferencePhase>;

class PlacementRemovalStrategy;
using PlacementRemovalStrategyPtr = std::shared_ptr<PlacementRemovalStrategy>;

/**
 * @brief This class takes as input a query plan (represented by a upstream and downstream operators) and removes the
 * placements for all intermediate operators that are in the state To-Be-Removed or To-Be-Replaced. Upon successful
 * removal of the placements the operator states are changed to Removed and To-Be-Placed respectively.
 *
 * It reflects the changes on the global execution plan by either marking a deployed query sub plan as Updated or Removed.
 *
 */
class PlacementRemovalStrategy {

  public:
    /**
     * @brief Create instance of placement removal strategy
     * @param globalExecutionPlan: the global execution plan to update
     * @param topology: the topology
     * @param typeInferencePhase: the type inference phase
     * @param placementAmendmentMode: the placement amended mode
     * @return a pointer to the placement
     */
    static PlacementRemovalStrategyPtr create(const GlobalExecutionPlanPtr& globalExecutionPlan,
                                              const TopologyPtr& topology,
                                              const TypeInferencePhasePtr& typeInferencePhase,
                                              PlacementAmendmentMode placementAmendmentMode);

    /**
     * Update Global execution plan by removing operators that are in the state To-Be-Removed and To-Be-Re-Placed
     * between input pinned upstream and downstream operators
     * @param sharedQueryId: id of the shared query
     * @param pinnedUpStreamOperators: pinned upstream operators
     * @param pinnedDownStreamOperators: pinned downstream operators
     * @param querySubPlanVersion: the new version of the updated query sub plans
     * @return true if successful else false
     */
    bool updateGlobalExecutionPlan(SharedQueryId sharedQueryId,
                                   const std::set<LogicalOperatorNodePtr>& pinnedUpStreamOperators,
                                   const std::set<LogicalOperatorNodePtr>& pinnedDownStreamOperators,
                                   QuerySubPlanVersion querySubPlanVersion);

    /**
     * @brief Destructor releases all locks (if any acquired) for pessimistic mode
     */
    ~PlacementRemovalStrategy();

  private:

    /**
     * Find topology path for placing operators between the input pinned upstream and downstream operators
     * @param upStreamPinnedOperators: the pinned upstream operators
     * @param downStreamPinnedOperators: the pinned downstream operators
     */
    void performPathSelection(const std::set<LogicalOperatorNodePtr>& upStreamPinnedOperators,
                              const std::set<LogicalOperatorNodePtr>& downStreamPinnedOperators);

    /**
     * @brief Select path for placement using pessimistic 2PL strategy. If attempt fails then an exponential retries are performed.
     * NOTE: These paths are local copies of the topology nodes. Any changes done on these nodes are not reflected in the topology catalog.
     * @return true if successful else false
     */
    bool pessimisticPathSelection();

    /**
     * @brief Perform unlocking of all topology nodes on which the lock was acquired.
     * We following an order inverse of the lock acquisition. This allows us to prevent starvation situation.
     * @return true if successful else false
     */
    bool unlockTopologyNodesInSelectedPath();

    /**
     * @brief Update the query sub plans by removing the query operators
     * @param sharedQueryId
     */
    void updateQuerySubPlans(SharedQueryId sharedQueryId);

    /**
     * @brief Add the computed query sub plans tot he global execution plan
     * @param sharedQueryId: the shared query plan id
     * @param querySubPlanVersion: the new version of the query sub plan
     * @return true if global execution plan gets updated successfully else false
     */
    bool updateExecutionNodes(SharedQueryId sharedQueryId, QuerySubPlanVersion querySubPlanVersion);

    PlacementRemovalStrategy(const GlobalExecutionPlanPtr& globalExecutionPlan,
                             const TopologyPtr& topology,
                             const TypeInferencePhasePtr& typeInferencePhase,
                             PlacementAmendmentMode placementMode);

    GlobalExecutionPlanPtr globalExecutionPlan;
    TopologyPtr topology;
    TypeInferencePhasePtr typeInferencePhase;
    PlacementAmendmentMode placementAmendmentMode;
    PathFinderPtr pathFinder;
    std::set<WorkerId> workerIdsInBFS;
    std::unordered_map<OperatorId, LogicalOperatorNodePtr> operatorIdToOriginalOperatorMap;
    std::unordered_map<WorkerId, uint32_t> workerIdToReleasedSlotMap;
    std::unordered_map<WorkerId, std::set<QuerySubPlanId>> workerIdToQuerySubPlanIds;
    std::unordered_map<WorkerId, std::vector<OperatorId>> workerIdToOperatorIdMap;
    std::unordered_map<WorkerId, std::vector<QueryPlanPtr>> workerIdToUpdatedQuerySubPlans;
    std::unordered_map<WorkerId, TopologyNodeWLock> lockedTopologyNodeMap;

    //Max retires for path selection before failing the placement
    static constexpr auto MAX_PATH_SELECTION_RETRIES = 3;
    //Time interval in which to retry
    static constexpr auto PATH_SELECTION_RETRY_WAIT = std::chrono::milliseconds(1000);
    static constexpr auto MAX_PATH_SELECTION_RETRY_WAIT = std::chrono::milliseconds(120000);
};

}// namespace Optimizer
}// namespace NES
#endif//NES_PLACEMENTREMOVALSTRATEGY_HPP

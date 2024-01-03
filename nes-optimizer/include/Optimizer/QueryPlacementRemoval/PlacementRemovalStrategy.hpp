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

#include <Configurations/Enums/PlacementAmenderMode.hpp>
#include <Identifiers.hpp>
#include <folly/Synchronized.h>
#include <memory>
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
     * @param topology : the topology
     * @param typeInferencePhase : the type inference phase
     * @param placementAmenderMode : the placement amender mode
     * @return a pointer to the placement
     */
    static PlacementRemovalStrategyPtr create(const GlobalExecutionPlanPtr& globalExecutionPlan,
                                              const TopologyPtr& topology,
                                              const TypeInferencePhasePtr& typeInferencePhase,
                                              PlacementAmenderMode placementAmenderMode);

    /**
     * Update Global execution plan by removing operators that are in the state To-Be-Removed and To-Be-Re-Placed
     * between input pinned upstream and downstream operators
     * @param sharedQueryId: id of the shared query
     * @param pinnedUpStreamOperators: pinned upstream operators
     * @param pinnedDownStreamOperators: pinned downstream operators
     * @return true if successful else false
     */
    bool updateGlobalExecutionPlan(SharedQueryId sharedQueryId,
                                   const std::set<LogicalOperatorNodePtr>& pinnedUpStreamOperators,
                                   const std::set<LogicalOperatorNodePtr>& pinnedDownStreamOperators);

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
     * @param topologyNodesWithUpStreamPinnedOperators : topology nodes hosting the pinned upstream operators
     * @param topologyNodesWithDownStreamPinnedOperators : topology nodes hosting the pinned downstream operators
     * @return true if successful else false
     */
    bool pessimisticPathSelection(const std::set<WorkerId>& topologyNodesWithUpStreamPinnedOperators,
                                  const std::set<WorkerId>& topologyNodesWithDownStreamPinnedOperators);

    /**
     * @brief Select path for placement using optimistic approach where no locks are acquired on chosen topology nodes
     * after the path selection. If attempt fails then an exponential retries are performed.
     * NOTE: These paths are local copies of the topology nodes. Any changes done on these nodes are not reflected in the topology catalog.
     * @param topologyNodesWithUpStreamPinnedOperators : topology nodes hosting the pinned upstream operators
     * @param topologyNodesWithDownStreamPinnedOperators : topology nodes hosting the pinned downstream operators
     */
    bool optimisticPathSelection(const std::set<WorkerId>& topologyNodesWithUpStreamPinnedOperators,
                                 const std::set<WorkerId>& topologyNodesWithDownStreamPinnedOperators);

    /**
     * @brief Perform locking of all topology nodes selected by the path selection algorithm.
     * We use "back-off and retry" mechanism to lock topology nodes following a strict breadth-first order.
     * This allows us to prevent deadlocks and starvation situation.
     * @param sourceTopologyNodes: the topology nodes hosting the pinned upstream operators
     * @return true if successful else false
     */
    bool lockTopologyNodesInSelectedPath(const std::vector<TopologyNodePtr>& sourceTopologyNodes);

    /**
     * @brief Perform unlocking of all topology nodes on which the lock was acquired.
     * We following an order inverse of the lock acquisition. This allows us to prevent starvation situation.
     * @return true if successful else false
     */
    bool unlockTopologyNodesInSelectedPath();

    PlacementRemovalStrategy(const GlobalExecutionPlanPtr& globalExecutionPlan,
                             const TopologyPtr& topology,
                             const TypeInferencePhasePtr& typeInferencePhase,
                             PlacementAmenderMode placementMode);

    GlobalExecutionPlanPtr globalExecutionPlan;
    TopologyPtr topology;
    TypeInferencePhasePtr typeInferencePhase;
    PlacementAmenderMode placementAmenderMode;
    PathFinderPtr pathFinder;
    std::vector<WorkerId> workerNodeIdsInBFS;
    std::set<WorkerId> pinnedUpStreamTopologyNodeIds;
    std::set<WorkerId> pinnedDownStreamTopologyNodeIds;
    std::unordered_map<WorkerId, TopologyNodePtr> workerIdToTopologyNodeMap;
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

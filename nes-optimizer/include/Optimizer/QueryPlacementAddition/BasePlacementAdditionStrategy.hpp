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

#ifndef NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYPLACEMENTADDITION_BASEPLACEMENTADDITIONSTRATEGY_HPP_
#define NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYPLACEMENTADDITION_BASEPLACEMENTADDITIONSTRATEGY_HPP_

#include <Catalogs/Source/SourceCatalogEntry.hpp>
#include <Configurations/Enums/PlacementAmendmentMode.hpp>
#include <Operators/Operator.hpp>
#include <Plans/Utils/PlanIdGenerator.hpp>
#include <Util/CopiedPinnedOperators.hpp>
#include <Util/FaultToleranceType.hpp>
#include <Util/CheckpointStorageType.hpp>
#include <Util/Placement/PlacementConstants.hpp>
#include <chrono>
#include <folly/Synchronized.h>
#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <unordered_map>
#include <vector>

namespace NES {

class Topology;
using TopologyPtr = std::shared_ptr<Topology>;

class TopologyNode;
using TopologyNodePtr = std::shared_ptr<TopologyNode>;

using TopologyNodeWLock = std::shared_ptr<folly::Synchronized<TopologyNodePtr>::WLockedPtr>;

class PathFinder;
using PathFinderPtr = std::shared_ptr<PathFinder>;

class Schema;
using SchemaPtr = std::shared_ptr<Schema>;

class LogicalOperator;
using LogicalOperatorPtr = std::shared_ptr<LogicalOperator>;

class Operator;
using OperatorPtr = std::shared_ptr<Operator>;

class SourceLogicalOperator;
using SourceLogicalOperatorPtr = std::shared_ptr<SourceLogicalOperator>;

class SinkLogicalOperator;
using SinkLogicalOperatorPtr = std::shared_ptr<SinkLogicalOperator>;

class DecomposedQueryPlan;
using DecomposedQueryPlanPtr = std::shared_ptr<DecomposedQueryPlan>;

namespace Catalogs::Source {
class SourceCatalog;
using SourceCatalogPtr = std::shared_ptr<SourceCatalog>;
}// namespace Catalogs::Source

namespace Optimizer {

class GlobalExecutionPlan;
using GlobalExecutionPlanPtr = std::shared_ptr<GlobalExecutionPlan>;

class ExecutionNode;
using ExecutionNodePtr = std::shared_ptr<ExecutionNode>;

class TypeInferencePhase;
using TypeInferencePhasePtr = std::shared_ptr<TypeInferencePhase>;

class BasePlacementAdditionStrategy;
using BasePlacementStrategyPtr = std::unique_ptr<BasePlacementAdditionStrategy>;

class DeploymentContext;
using DeploymentContextPtr = std::shared_ptr<DeploymentContext>;

struct PlacementAdditionResult {
    PlacementAdditionResult(bool completedSuccessfully,
                            std::unordered_map<DecomposedQueryId, DeploymentContextPtr> deploymentContexts)
        : completedSuccessfully(completedSuccessfully), deploymentContexts(deploymentContexts){};

    bool completedSuccessfully;
    std::unordered_map<DecomposedQueryId, DeploymentContextPtr> deploymentContexts;
};

using PlacementMatrix = std::vector<std::vector<bool>>;
using ComputedDecomposedQueryPlans = std::unordered_map<WorkerId, std::vector<DecomposedQueryPlanPtr>>;
using ExecutionNodeWLock = std::shared_ptr<folly::Synchronized<ExecutionNodePtr>::WLockedPtr>;

/**
 * @brief: This is the interface for base optimizer that needed to be implemented by any new query optimizer.
 */
class BasePlacementAdditionStrategy {

  public:
    explicit BasePlacementAdditionStrategy(const GlobalExecutionPlanPtr& globalExecutionPlan,
                                           const TopologyPtr& topology,
                                           const TypeInferencePhasePtr& typeInferencePhase,
                                           PlacementAmendmentMode placementAmendmentMode);

    virtual ~BasePlacementAdditionStrategy();

    /**
     * Update Global execution plan by placing operators including and between input pinned upstream and downstream operators
     * for the query with input id and input fault tolerance strategy
     * @param sharedQueryId: id of the shared query
     * @param pinnedUpStreamOperators: pinned upstream operators
     * @param pinnedDownStreamOperators: pinned downstream operators
     * @param querySubPlanVersion: the new version of the updated query sub plans
     * @param faultTolerance fault tolerance type
     * @param checkpointStorage storage option if checkpointing enabled
     * @return vector of deployment contexts
     */
    virtual PlacementAdditionResult updateGlobalExecutionPlan(SharedQueryId sharedQueryId,
                                                              const std::set<LogicalOperatorPtr>& pinnedUpStreamOperators,
                                                              const std::set<LogicalOperatorPtr>& pinnedDownStreamOperators,
                                                              DecomposedQueryPlanVersion querySubPlanVersion,
                                                              FaultToleranceType faultTolerance = FaultToleranceType::NONE,
                                                              CheckpointStorageType checkpointStorage = CheckpointStorageType::NONE) = 0;

  protected:
    /**
     * Find topology path for placing operators between the input pinned upstream and downstream operators
     * @param upStreamPinnedOperators: the pinned upstream operators
     * @param downStreamPinnedOperators: the pinned downstream operators
     */
    void performPathSelection(const std::set<LogicalOperatorPtr>& upStreamPinnedOperators,
                              const std::set<LogicalOperatorPtr>& downStreamPinnedOperators);

    /**
     * @brief Iterate through operators between pinnedUpStreamOperators and pinnedDownStreamOperators and compute
     * decomposed query plans on the designated topology node
     * @param sharedQueryId the shared query plan id
     * @param pinnedUpStreamOperators the upstream operators
     * @param pinnedDownStreamOperators the downstream operators
     */
    ComputedDecomposedQueryPlans computeDecomposedQueryPlans(SharedQueryId sharedQueryId,
                                                             const std::set<LogicalOperatorPtr>& pinnedUpStreamOperators,
                                                             const std::set<LogicalOperatorPtr>& pinnedDownStreamOperators);

    /**
     * @brief Add network source and sink operators
     * @param computedDecomposedQueryPlans: update the computed sub query plan by adding network source and sink operators
     */
    void addNetworkOperators(ComputedDecomposedQueryPlans& computedDecomposedQueryPlans);

    /**
     * @brief Add the computed query sub plans tot he global execution plan
     * @param sharedQueryId: the shared query plan id
     * @param computedSubQueryPlans: the computed query sub plans
     * @param decomposedQueryPlanVersion: the version of the query sub plan
     * @param faultTolerance fault tolerance type
     * @param checkpointStorage storage option if checkpointing enabled
     * @return vector of deployment contexts
     */
    PlacementAdditionResult updateExecutionNodes(SharedQueryId sharedQueryId,
                                                 ComputedDecomposedQueryPlans& computedSubQueryPlans,
                                                 DecomposedQueryPlanVersion decomposedQueryPlanVersion,
                                                 FaultToleranceType faultTolerance,
                                                 CheckpointStorageType checkpointStorage);

    /**
     * @brief Get the Topology node with the input id
     * @param workerId: the id of the topology node
     * @return Topology node ptr or nullptr
     */
    TopologyNodePtr getTopologyNode(WorkerId workerId);

    /**
     * @brief Get topology node where all children operators of the input operator are placed
     * @param operatorNode: the input operator
     * @return vector of topology nodes where child operator was placed or empty if not all children operators are placed
     */
    std::vector<TopologyNodePtr> getTopologyNodesForChildrenOperators(const LogicalOperatorPtr& operatorNode);

    /**
     * @brief Perform locking of all topology nodes selected by the path selection algorithm.
     * We use "back-off and retry" mechanism to lock topology nodes following a strict breadth-first order.
     * This allows us to prevent deadlocks and starvation situation.
     * @return true if successful else false
     */
    bool lockTopologyNodesInSelectedPath();

    /**
     * @brief Perform unlocking of all topology nodes on which the lock was acquired.
     * We following an order inverse of the lock acquisition. This allows us to prevent starvation situation.
     * @return true if successful else false
     */
    bool unlockTopologyNodesInSelectedPath();

    GlobalExecutionPlanPtr globalExecutionPlan;
    TopologyPtr topology;
    TypeInferencePhasePtr typeInferencePhase;
    PathFinderPtr pathFinder;
    PlacementAmendmentMode placementAmendmentMode;
    std::unordered_map<WorkerId, TopologyNodePtr> workerIdToTopologyNodeMap;
    std::unordered_map<OperatorId, LogicalOperatorPtr> operatorIdToOriginalOperatorMap;
    std::vector<WorkerId> workerNodeIdsInBFS;
    std::unordered_map<WorkerId, TopologyNodeWLock> lockedTopologyNodeMap;

  private:
    /**
     * @brief create a new network sink operator
     * @param sharedQueryId : the query id to which the sink belongs to
     * @param sourceOperatorId : the operator id of the corresponding source operator
     * @param sourceTopologyNode : the topology node to which sink operator will send the data
     * @return the instance of network sink operator
     */
    static LogicalOperatorPtr createNetworkSinkOperator(SharedQueryId sharedQueryId,
                                                        OperatorId sourceOperatorId,
                                                        const TopologyNodePtr& sourceTopologyNode);

    /**
     * @brief create a new network source operator
     * @param sharedQueryId : the query id to which the source belongs to
     * @param inputSchema : the schema for input event stream
     * @param operatorId : the operator id of the source network operator
     * @param sinkTopologyNode: sink topology node
     * @return the instance of network source operator
     */
    static LogicalOperatorPtr createNetworkSourceOperator(SharedQueryId sharedQueryId,
                                                          SchemaPtr inputSchema,
                                                          OperatorId operatorId,
                                                          const TopologyNodePtr& sinkTopologyNode);

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
     * @brief call the appropriate find path method on the topology to select the placement path
     * @param topologyNodesWithUpStreamPinnedOperators : topology nodes hosting the pinned upstream operators
     * @param topologyNodesWithDownStreamPinnedOperators : topology nodes hosting the pinned downstream operators
     * @return the upstream topology nodes of the selected path
     */
    std::vector<TopologyNodePtr> findPath(const std::set<WorkerId>& topologyNodesWithUpStreamPinnedOperators,
                                          const std::set<WorkerId>& topologyNodesWithDownStreamPinnedOperators);

  private:
    /**
     * @brief Update state of all operators that were placed but as placed
     * @param workerId: the id of the worker where the operator is placed
     * @param decomposedQueryPlan: the  decomposed query plan updated by the placement addition strategy
     */
    void markOperatorsAsPlaced(WorkerId workerId, DecomposedQueryPlanPtr decomposedQueryPlan);

    /**
     * @brief check if a computed sink operator corresponds to a placed sink that is to be reconfigured. If so,
     * update the version, the receiver location and the partition of the placed sink
     * @param querySubPlanVersion the version to apply to the reconfigured sink
     * @param computedQuerySubPlan the sub plan containing the newly computed changed to be applied to the placed plans
     * @param upstreamOperatorOfPlacedSinksToCheck the placed operator whose parents are to be checked if they can be
     * merged with the new sink
     * @param newNetworkSinkOperator the new sink which is to be checked if ti can be merged with an existing sink
     * @return true if merging was performed, false if no matching sink could be found
     */
    bool tryMergingNetworkSink(DecomposedQueryPlanVersion querySubPlanVersion,
                               const DecomposedQueryPlanPtr& computedQuerySubPlan,
                               const NodePtr& upstreamOperatorOfPlacedSinksToCheck,
                               const SinkLogicalOperatorPtr& newNetworkSinkOperator);

    /**
     * @brief chack if a computed source operator corresponds to a placed source that is to be reconfigured. If so,
     * update the version and the sender location of the placed source
     * @param querySubPlanVersion the version to apply to the reconfigured sink
     * @param placedDownstreamOperator
     * @param newNetworkSourceOperator
     * @return
     */
    bool tryMergingNetworkSource(DecomposedQueryPlanVersion querySubPlanVersion,
                                 const NodePtr& placedDownstreamOperator,
                                 const SourceLogicalOperatorPtr& newNetworkSourceOperator);

    //Number of retries to connect to downstream source operators
    static constexpr auto SINK_RETRIES = 100;
    //Time interval in which to retry
    static constexpr auto SINK_RETRY_WAIT = std::chrono::milliseconds(10);
    //Number of retries to connect to upstream sink operators
    static constexpr auto SOURCE_RETRIES = 100;
    //Time interval in which to retry
    static constexpr auto SOURCE_RETRY_WAIT = std::chrono::milliseconds(10);
    // Metadata
    std::set<WorkerId> pinnedUpStreamTopologyNodeIds;
    std::set<WorkerId> pinnedDownStreamTopologyNodeIds;
    std::unordered_map<WorkerId, uint16_t> workerIdToResourceConsumedMap;
    std::unordered_map<OperatorId, LogicalOperatorPtr> operatorIdToCopiedOperatorMap;
};
}// namespace Optimizer
}// namespace NES
#endif// NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYPLACEMENTADDITION_BASEPLACEMENTADDITIONSTRATEGY_HPP_

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

#ifndef NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYPLACEMENT_BASEPLACEMENTSTRATEGY_HPP_
#define NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYPLACEMENT_BASEPLACEMENTSTRATEGY_HPP_

#include <Catalogs/Source/SourceCatalogEntry.hpp>
#include <Plans/Utils/PlanIdGenerator.hpp>
#include <Configurations/Enums/PlacementAmenderMode.hpp>
#include <chrono>
#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <unordered_map>
#include <vector>

namespace NES {

class NESExecutionPlan;
using NESExecutionPlanPtr = std::shared_ptr<NESExecutionPlan>;

class ExecutionNode;
using ExecutionNodePtr = std::shared_ptr<ExecutionNode>;

class QueryPlan;
using QueryPlanPtr = std::shared_ptr<QueryPlan>;

class Topology;
using TopologyPtr = std::shared_ptr<Topology>;

class Schema;
using SchemaPtr = std::shared_ptr<Schema>;

class LogicalOperatorNode;
using LogicalOperatorNodePtr = std::shared_ptr<LogicalOperatorNode>;

class GlobalExecutionPlan;
using GlobalExecutionPlanPtr = std::shared_ptr<GlobalExecutionPlan>;

class OperatorNode;
using OperatorNodePtr = std::shared_ptr<OperatorNode>;

class SourceLogicalOperatorNode;
using SourceLogicalOperatorNodePtr = std::shared_ptr<SourceLogicalOperatorNode>;

class NetworkSinkDescriptor;
using NetworkSinkDescriptorPtr = std::shared_ptr<NetworkSinkDescriptor>;

namespace Catalogs::Source {
class SourceCatalog;
using SourceCatalogPtr = std::shared_ptr<SourceCatalog>;
}// namespace Catalogs::Source
}// namespace NES

namespace NES::Optimizer {

class TypeInferencePhase;
using TypeInferencePhasePtr = std::shared_ptr<TypeInferencePhase>;

class BasePlacementStrategy;
using BasePlacementStrategyPtr = std::unique_ptr<BasePlacementStrategy>;

using PlacementMatrix = std::vector<std::vector<bool>>;

const std::string PINNED_WORKER_ID = "PINNED_WORKER_ID";// Property indicating the location where the operator is pinned
const std::string PROCESSED = "PROCESSED";              // Property indicating if operator was processed for placement
const std::string CO_LOCATED_UPSTREAM_OPERATORS = "CO_LOCATED_UPSTREAM_OPERATORS";              // Property indicating if operator was processed for placement

using ComputedSubQueryPlans = std::unordered_map<WorkerId, std::vector<QueryPlanPtr>>;

/**
 * @brief: This is the interface for base optimizer that needed to be implemented by any new query optimizer.
 */
class BasePlacementStrategy {

  public:
    explicit BasePlacementStrategy(const GlobalExecutionPlanPtr& globalExecutionPlan,
                                   const TopologyPtr& topology,
                                   const TypeInferencePhasePtr& typeInferencePhase,
                                   PlacementAmenderMode placementMode);

    virtual ~BasePlacementStrategy() = default;

    /**
     * Update Global Execution plan by placing the input query plan
     * @param queryPlan: the query plan to place
     * @return true if successful else false
     * @throws QueryPlacementException
     */
    virtual bool updateGlobalExecutionPlan(QueryPlanPtr queryPlan);

    /**
     * Update Global execution plan by placing operators including and between input pinned upstream and downstream operators
     * for the query with input id and input fault tolerance strategy
     * @param sharedQueryId: id of the shared query
     * @param pinnedUpStreamOperators: pinned upstream operators
     * @param pinnedDownStreamOperators: pinned downstream operators
     * @return true if successful else false
     */
    virtual bool updateGlobalExecutionPlan(SharedQueryId sharedQueryId,
                                           const std::set<LogicalOperatorNodePtr>& pinnedUpStreamOperators,
                                           const std::set<LogicalOperatorNodePtr>& pinnedDownStreamOperators) = 0;

    /**
     * @brief Sets pinned node to the operator's property
     * @param queryPlan query plan containing operator to pin
     * @param topology topology containing node to pin
     * @param matrix 2D matrix containing the pinning information
     */
    static void pinOperators(QueryPlanPtr queryPlan, const TopologyPtr& topology, NES::Optimizer::PlacementMatrix& matrix);

  protected:
    /**
     * Find topology path for placing operators between the input pinned upstream and downstream operators
     * @param upStreamPinnedOperators: the pinned upstream operators
     * @param downStreamPinnedOperators: the pinned downstream operators
     */
    void performPathSelection(const std::set<LogicalOperatorNodePtr>& upStreamPinnedOperators,
                              const std::set<LogicalOperatorNodePtr>& downStreamPinnedOperators);

    /**
     * @brief Iterate through operators between pinnedUpStreamOperators and pinnedDownStreamOperators and compute query
     * sub plans on the designated topology node
     * @param sharedQueryId the shared query plan id
     * @param pinnedUpStreamOperators the upstream operators
     * @param pinnedDownStreamOperators the downstream operators
     */
    ComputedSubQueryPlans computeQuerySubPlans(SharedQueryId sharedQueryId,
                                               const std::set<LogicalOperatorNodePtr>& pinnedUpStreamOperators,
                                               const std::set<LogicalOperatorNodePtr>& pinnedDownStreamOperators);

    /**
     * @brief Add network source and sink operators
     * @param computedSubQueryPlans: update the computed sub query plan by adding network source and sink operators
     */
    void addNetworkOperators(ComputedSubQueryPlans& computedSubQueryPlans);

    /**
     * @brief Add the computed query sub plans tot he global execution plan
     * @param sharedQueryId: the shared query plan id
     * @param computedSubQueryPlans: the computed query sub plans
     * @return true if global execution plan gets updated successfully else false
     */
    bool updateExecutionNodes(SharedQueryId sharedQueryId, ComputedSubQueryPlans& computedSubQueryPlans);

    /**
     * @brief Get Execution node for the input topology node
     * @param candidateTopologyNode: topology node
     * @return Execution Node pointer
     */
    ExecutionNodePtr getExecutionNode(const TopologyNodePtr& candidateTopologyNode);

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
    std::vector<TopologyNodePtr> getTopologyNodesForChildrenOperators(const LogicalOperatorNodePtr& operatorNode);

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
    bool unlockTopologyNodes();

    GlobalExecutionPlanPtr globalExecutionPlan;
    TopologyPtr topology;
    TypeInferencePhasePtr typeInferencePhase;
    PlacementAmenderMode placementAmenderMode;
    std::vector<WorkerId> workerNodeIdsInBFS;
    std::set<WorkerId> pinnedUpStreamTopologyNodeIds;
    std::set<WorkerId> pinnedDownStreamTopologyNodeIds;
    std::unordered_map<WorkerId, TopologyNodePtr> workerIdToTopologyNodeMap;
    std::unordered_map<WorkerId, uint16_t> workerIdToResourceConsumedMap;
    std::unordered_map<OperatorId, LogicalOperatorNodePtr> operatorIdToOperatorMap;
    std::unordered_map<WorkerId, std::vector<LogicalOperatorNodePtr>> workerIdToPinnedOperatorMap;

  private:
    /**
     * @brief create a new network sink operator
     * @param queryId : the query id to which the sink belongs to
     * @param sourceOperatorId : the operator id of the corresponding source operator
     * @param sourceTopologyNode : the topology node to which sink operator will send the data
     * @return the instance of network sink operator
     */
    static LogicalOperatorNodePtr
    createNetworkSinkOperator(QueryId queryId, OperatorId sourceOperatorId, const TopologyNodePtr& sourceTopologyNode);

    /**
     * @brief create a new network source operator
     * @param queryId : the query id to which the source belongs to
     * @param inputSchema : the schema for input event stream
     * @param operatorId : the operator id of the source network operator
     * @param sinkTopologyNode: sink topology node
     * @return the instance of network source operator
     */
    static LogicalOperatorNodePtr createNetworkSourceOperator(QueryId queryId,
                                                              SchemaPtr inputSchema,
                                                              OperatorId operatorId,
                                                              const TopologyNodePtr& sinkTopologyNode);

    /**
     * @brief Select path for placement using pessimistic 2PL strategy
     * @param topologyNodesWithUpStreamPinnedOperators : topology nodes hosting the pinned upstream operators
     * @param topologyNodesWithDownStreamPinnedOperators : topology nodes hosting the pinned downstream operators
     */
    void pessimisticPathSelection(const std::set<WorkerId>& topologyNodesWithUpStreamPinnedOperators,
                                  const std::set<WorkerId>& topologyNodesWithDownStreamPinnedOperators);

    /**
     * @brief Select path for placement using optimistic approach where no locks are acquired on chosen topology nodes
     * after the path selection
     * @param topologyNodesWithUpStreamPinnedOperators : topology nodes hosting the pinned upstream operators
     * @param topologyNodesWithDownStreamPinnedOperators : topology nodes hosting the pinned downstream operators
     */
    void optimisticPathSelection(const std::set<WorkerId>& topologyNodesWithUpStreamPinnedOperators,
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
    //Number of retries to connect to downstream source operators
    static constexpr auto SINK_RETRIES = 100;
    //Time interval in which to retry
    static constexpr auto SINK_RETRY_WAIT = std::chrono::milliseconds(10);
    //Number of retries to connect to upstream sink operators
    static constexpr auto SOURCE_RETRIES = 100;
    //Time interval in which to retry
    static constexpr auto SOURCE_RETRY_WAIT = std::chrono::milliseconds(10);
    //Max retires for path selection before failing the placement
    static constexpr auto MAX_PATH_SELECTION_RETRIES = 3;
    //Time interval in which to retry
    static constexpr auto PATH_SELECTION_RETRY_WAIT = std::chrono::milliseconds(1000);
    static constexpr auto MAX_PATH_SELECTION_RETRY_WAIT = std::chrono::milliseconds(120000);
};
}// namespace NES::Optimizer
#endif// NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYPLACEMENT_BASEPLACEMENTSTRATEGY_HPP_

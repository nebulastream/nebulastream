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
#include <Util/PlacementMode.hpp>
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

const std::string PINNED_WORKER_ID = "PINNED_NODE_ID";// Property indicating the location where the operator is pinned
const std::string PROCESSED = "PROCESSED";            // Property indicating if operator was processed for placement

/**
 * @brief: This is the interface for base optimizer that needed to be implemented by any new query optimizer.
 */
class BasePlacementStrategy {

  public:
    explicit BasePlacementStrategy(const GlobalExecutionPlanPtr& globalExecutionPlan,
                                   const TopologyPtr& topology,
                                   const TypeInferencePhasePtr& typeInferencePhase,
                                   PlacementMode placementMode);

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
     * @brief Iterate through operators between pinnedUpStreamOperators and pinnedDownStreamOperators and assign them to the
     * designated topology node
     * @param sharedQueryId id of the query containing operators to place
     * @param pinnedUpStreamOperators the upstream operators preceeding all operators to place
     * @param pinnedDownStreamOperators the downstream operators succeeding all operators to place
     */
    void placePinnedOperators(SharedQueryId sharedQueryId,
                              const std::set<LogicalOperatorNodePtr>& pinnedUpStreamOperators,
                              const std::set<LogicalOperatorNodePtr>& pinnedDownStreamOperators);

    /**
     * @brief Iterate through operators between pinnedUpStreamOperators and pinnedDownStreamOperators and compute query
     * sub plans on the designated topology node
     * @param pinnedUpStreamOperators the upstream operators
     * @param pinnedDownStreamOperators the downstream operators
     */
    void computeQuerySubPlans(const std::set<LogicalOperatorNodePtr>& pinnedUpStreamOperators,
                              const std::set<LogicalOperatorNodePtr>& pinnedDownStreamOperators);

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
     * @brief Add network source and sinks between query sub plans allocated on different execution nodes
     * @param sharedQueryId: the shared query plan
     * @param pinnedUpStreamOperators: the upstream operators of the query plan
     * @param pinnedDownStreamOperators: the downstream operators of the query plan
     */
    void addNetworkSourceAndSinkOperators(SharedQueryId sharedQueryId,
                                          const std::set<LogicalOperatorNodePtr>& pinnedUpStreamOperators,
                                          const std::set<LogicalOperatorNodePtr>& pinnedDownStreamOperators);

    /**
     * @brief Run the type inference phase for all the query sub plans for the input query id
     * @param queryId: the input query id
     * @return true if successful else false
     */
    bool runTypeInferencePhase(QueryId queryId);

    /**
     * @brief Get topology node where all children operators of the input operator are placed
     * @param operatorNode: the input operator
     * @return vector of topology nodes where child operator was placed or empty if not all children operators are placed
     */
    std::vector<TopologyNodePtr> getTopologyNodesForChildrenOperators(const LogicalOperatorNodePtr& operatorNode);

    /**
     * @brief Get the candidate query plan where input operator is to be appended
     * @param queryId : the query id
     * @param operatorNode : the candidate operator
     * @param executionNode : the execution node where operator is to be placed
     * @return the query plan to which the input operator is to be appended
     */
    static QueryPlanPtr getCandidateQueryPlanForOperator(SharedQueryId sharedQueryId,
                                                         const LogicalOperatorNodePtr& operatorNode,
                                                         const ExecutionNodePtr& executionNode);

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
    PlacementMode placementMode;
    std::map<WorkerId, TopologyNodePtr> topologyMap;
    std::map<OperatorId, ExecutionNodePtr> operatorToExecutionNodeMap;
    std::unordered_map<OperatorId, QueryPlanPtr> operatorToSubPlan;
    std::vector<WorkerId> lockedTopologyNodeIds;
    std::unordered_map<WorkerId, LogicalOperatorNodePtr> workerIdToRootOperatorMap;

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
     * @brief Attach network source or sink operator to the given operator
     * @param queryId : the id of the query
     * @param upStreamOperator : the logical operator to which source or sink operator need to be attached
     * @param pinnedDownStreamOperators: the collection of pinned downstream operator after which to stop
     */
    void placeNetworkOperator(QueryId queryId,
                              const LogicalOperatorNodePtr& upStreamOperator,
                              const std::set<LogicalOperatorNodePtr>& pinnedDownStreamOperators);

    /**
     * Check if operator present in the given collection
     * @param operatorToSearch : operator to search
     * @param operatorCollection : collection to search into
     * @return true if successful
     */
    bool operatorPresentInCollection(const LogicalOperatorNodePtr& operatorToSearch,
                                     const std::set<LogicalOperatorNodePtr>& operatorCollection);

    /**
     * @brief Add an execution node as root of the global execution plan
     * @param executionNode: execution node to add as root
     */
    void addExecutionNodeAsRoot(ExecutionNodePtr& executionNode);

    /**
     * @brief This method validates if the given upStream and downStream operators are connected to each other directly or via
     * intermediate query plans
     * @param upStreamOperator : The up stream operator
     * @param downStreamOperator : The down stream operator
     * @return true if operators connected otherwise false
     */
    bool isSourceAndDestinationConnected(const LogicalOperatorNodePtr& upStreamOperator,
                                         const LogicalOperatorNodePtr& downStreamOperator);

    /**
     * @brief Select path for placement using pessimistic 2PL strategy
     * @param topologyNodesWithUpStreamPinnedOperators : topology nodes hosting the pinned upstream operators
     * @param topologyNodesWithDownStreamPinnedOperators : topology nodes hosting the pinned downstream operators
     */
    void pessimisticPathSelection(const std::set<TopologyNodePtr>& topologyNodesWithUpStreamPinnedOperators,
                                  const std::set<TopologyNodePtr>& topologyNodesWithDownStreamPinnedOperators);

    /**
     * @brief Select path for placement using optimistic approach where no locks are acquired on chosen topology nodes
     * after the path selection
     * @param topologyNodesWithUpStreamPinnedOperators : topology nodes hosting the pinned upstream operators
     * @param topologyNodesWithDownStreamPinnedOperators : topology nodes hosting the pinned downstream operators
     */
    void optimisticPathSelection(const std::set<TopologyNodePtr>& topologyNodesWithUpStreamPinnedOperators,
                                 const std::set<TopologyNodePtr>& topologyNodesWithDownStreamPinnedOperators);

    /**
     * @brief call the appropriate find path method on the topology to select the placement path
     * @param topologyNodesWithUpStreamPinnedOperators : topology nodes hosting the pinned upstream operators
     * @param topologyNodesWithDownStreamPinnedOperators : topology nodes hosting the pinned downstream operators
     * @return the upstream topology nodes of the selected path
     */
    std::vector<TopologyNodePtr> findPath(const std::set<TopologyNodePtr>& topologyNodesWithUpStreamPinnedOperators,
                                          const std::set<TopologyNodePtr>& topologyNodesWithDownStreamPinnedOperators);

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

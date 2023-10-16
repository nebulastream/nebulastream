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

#ifndef NES_CORE_INCLUDE_OPTIMIZER_QUERYPLACEMENT_BASEPLACEMENTSTRATEGY_HPP_
#define NES_CORE_INCLUDE_OPTIMIZER_QUERYPLACEMENT_BASEPLACEMENTSTRATEGY_HPP_

#include <Catalogs/Source/SourceCatalogEntry.hpp>

#include <Plans/Utils/PlanIdGenerator.hpp>
#include <Util/FaultTolerancePlacement.hpp>
#include <Util/FaultToleranceType.hpp>
#include <Util/QueryType.hpp>
#include <Util/LineageType.hpp>
#include <chrono>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <unordered_map>
#include <vector>

namespace NES {

struct resourcesPerPath {
    double maxMemory;
    double minMemory;
    double requiredMemory;
    double maxNetwork;
    double minNetwork;
    double requiredNetwork;
    double providedSafety;
};

typedef struct resourcesPerPath ResourcesPerPath;

struct placementScore {
    double score;
    std::vector<TopologyNodePtr> path;
    std::map<uint64_t, std::pair<double, double>> individualNodeScores;
};

typedef struct placementScore PlacementScore;

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

const std::string PINNED_NODE_ID = "PINNED_NODE_ID";
const std::string PLACED = "PLACED";
const double TUPLE_SIZE = 0.131;

/**
 * @brief: This is the interface for base optimizer that needed to be implemented by any new query optimizer.
 */
class BasePlacementStrategy {

  private:
    //Number of retries to connect to downstream source operators
    static constexpr auto SINK_RETRIES = 100;
    //Time interval in which to retry
    static constexpr auto SINK_RETRY_WAIT = std::chrono::milliseconds(10);
    //Number of retries to connect to upstream sink operators
    static constexpr auto SOURCE_RETRIES = 100;
    //Time interval in which to retry
    static constexpr auto SOURCE_RETRY_WAIT = std::chrono::milliseconds(10);

  public:
    explicit BasePlacementStrategy(GlobalExecutionPlanPtr globalExecutionPlan,
                                   TopologyPtr topologyPtr,
                                   TypeInferencePhasePtr typeInferencePhase);

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
     * @param queryId: id of the query
     * @param faultToleranceType: fault tolerance type
     * @param lineageType: lineage type
     * @param ftPlacement: fault tolerance placement
     * @param pinnedUpStreamOperators: pinned upstream operators
     * @param pinnedDownStreamOperators: pinned downstream operators
     * @return true if successful else false
     */
    virtual bool updateGlobalExecutionPlan(QueryId queryId,
                                           FaultToleranceType::Value faultToleranceType,
                                           LineageType::Value lineageType,
                                           FaultTolerancePlacement::Value ftPlacement,
                                           const std::vector<OperatorNodePtr>& pinnedUpStreamOperators,
                                           const std::vector<OperatorNodePtr>& pinnedDownStreamOperators) = 0;

    /**
     * @brief Sets pinned node to the operator's property
     * @param queryPlan query plan containing operator to pin
     * @param topology topology containing node to pin
     * @param matrix 2D matrix containing the pinning information
     */
    static void pinOperators(QueryPlanPtr queryPlan, TopologyPtr topology, NES::Optimizer::PlacementMatrix& matrix);

  protected:
    /**
     * Find topology path for placing operators between the input pinned upstream and downstream operators
     * @param upStreamPinnedOperators: the pinned upstream operators
     * @param downStreamPinnedOperators: the pinned downstream operators
     */
    void performPathSelection(const std::vector<OperatorNodePtr>& upStreamPinnedOperators,
                              const std::vector<OperatorNodePtr>& downStreamPinnedOperators,
                              FaultToleranceType::Value faultToleranceType,
                              FaultTolerancePlacement::Value ftPlacement);

    /**
     * @brief Iterate through operators between pinnedUpStreamOperators and pinnedDownStreamOperators and assign them to the
     * designated topology node
     * @param queryId id of the query containing operators to place
     * @param pinnedUpStreamOperators the upstream operators preceeding operators to place
     * @param pinnedDownStreamOperators the downstream operators succeeding operators to place
     */
    void placePinnedOperators(QueryId queryId,
                              const std::vector<OperatorNodePtr>& pinnedUpStreamOperators,
                              const std::vector<OperatorNodePtr>& pinnedDownStreamOperators);

    /**
     * @brief Get Execution node for the input topology node
     * @param candidateTopologyNode: topology node
     * @return Execution Node pointer
     */
    ExecutionNodePtr getExecutionNode(const TopologyNodePtr& candidateTopologyNode);

    /**
     * @brief Get the Topology node with the input id
     * @param nodeId: the id of the topology node
     * @return Topology node ptr or nullptr
     */
    TopologyNodePtr getTopologyNode(uint64_t nodeId);

    /**
     * @brief Add network source and sinks between query sub plans allocated on different execution nodes
     * @param queryPlan: the original query plan
     */
    void addNetworkSourceAndSinkOperators(QueryId queryId,
                                          const std::vector<OperatorNodePtr>& pinnedUpStreamOperators,
                                          const std::vector<OperatorNodePtr>& pinnedDownStreamOperators);

    /**
     * @brief Run the type inference phase for all the query sub plans for the input query id
     * @param queryId: the input query id
     * @param faultToleranceType: fault-tolerance type
     * @param lineageType: lineage type
     * @return true if successful else false
     */
    bool runTypeInferencePhase(QueryId queryId, FaultToleranceType::Value faultToleranceType, LineageType::Value lineageType);

    /**
     * @brief Get topology node where all children operators of the input operator are placed
     * @param operatorNode: the input operator
     * @return vector of topology nodes where child operator was placed or empty if not all children operators are placed
     */
    std::vector<TopologyNodePtr> getTopologyNodesForChildrenOperators(const OperatorNodePtr& operatorNode);

    /**
     * @brief Get the candidate query plan where input operator is to be appended
     * @param queryId : the query id
     * @param operatorNode : the candidate operator
     * @param executionNode : the execution node where operator is to be placed
     * @return the query plan to which the input operator is to be appended
     */
    static QueryPlanPtr
    getCandidateQueryPlanForOperator(QueryId queryId, const OperatorNodePtr& operatorNode, const ExecutionNodePtr& executionNode);

    GlobalExecutionPlanPtr globalExecutionPlan;
    TopologyPtr topology;
    TypeInferencePhasePtr typeInferencePhase;
    std::map<uint64_t, TopologyNodePtr> topologyMap;
    std::map<uint64_t, ExecutionNodePtr> operatorToExecutionNodeMap;
    std::unordered_map<OperatorId, QueryPlanPtr> operatorToSubPlan;
    QueryType::Value queryType;
    double w_memory = 0.33;
    double w_network = 0.33;
    double w_safety = 0.33;

    std::function<void(std::vector<TopologyNodePtr>)> adaptEpochCallback;
    std::function<void(QueryType::Value queryType, FaultToleranceType::Value ftType)> adjustWeightsCallback;

  private:
    /**
     * @brief create a new network sink operator
     * @param queryId : the query id to which the sink belongs to
     * @param sourceOperatorId : the operator id of the corresponding source operator
     * @param sourceTopologyNode : the topology node to which sink operator will send the data
     * @return the instance of network sink operator
     */
    static OperatorNodePtr createNetworkSinkOperator(QueryId queryId,
                                                     uint64_t sourceOperatorId,
                                                     const TopologyNodePtr& sourceTopologyNode,
                                                     const TopologyNodePtr& currentNode);

    /**
     * @brief create a new network source operator
     * @param queryId : the query id to which the source belongs to
     * @param inputSchema : the schema for input event stream
     * @param operatorId : the operator id of the source network operator
     * @param sinkTopologyNode: sink topology node
     * @return the instance of network source operator
     */
    static OperatorNodePtr createNetworkSourceOperator(QueryId queryId,
                                                       SchemaPtr inputSchema,
                                                       uint64_t operatorId,
                                                       const TopologyNodePtr& sinkTopologyNode);

    /**
     * @brief Attach network source or sink operator to the given operator
     * @param queryId : the id of the query
     * @param upStreamOperator : the logical operator to which source or sink operator need to be attached
     * @param pinnedDownStreamOperators: the collection of pinned downstream operator after which to stop
     */
    void placeNetworkOperator(QueryId queryId,
                              const OperatorNodePtr& upStreamOperator,
                              const std::vector<OperatorNodePtr>& pinnedDownStreamOperators);

    /**
     * @brief place fault tolerance in naive way
     * @param availablePaths : available paths between pinned operators
     */
    std::vector<TopologyNodePtr> placeFaultToleranceNaive(std::vector<std::vector<TopologyNodePtr>> availablePaths);

    /**
     * @brief place fault tolerance in elaborative way
     * @param availablePaths : available paths between pinned operators
     * @param faultToleranceType : provided level of reliability
     * @return selected placement
     */
    std::vector<TopologyNodePtr> placeFaultToleranceMFTP(std::vector<std::vector<TopologyNodePtr>> availablePaths, FaultToleranceType::Value faultToleranceType);

    /**
     * @brief place fault tolerance in elaborative way
     * @param subPathForPlacement : available paths between pinned operators
     * @param originalPath: original path
     * @return placement score
     */
    PlacementScore placeFaultTolerance(std::vector<TopologyNodePtr> subPathForPlacement, const std::vector<TopologyNodePtr> originalPath);

    /**
     * Calculates the score based on the node location on a path
     * @param pathForPlacement : path that is considered for placement
     * @param topologyNode : topology node
     * @return score that equals distance from the root node
     */
    uint64_t distanceScore(std::vector<TopologyNodePtr> pathForPlacement, const TopologyNodePtr& topologyNode);

    /**
     * Calculates the optimal epoch parameter considering the smallest device on the path
     * @param pathForPlacement path that was chosen for placement
     */
    void adaptEpoch(std::vector<TopologyNodePtr> pathForPlacement);

    /**
     * Calculates the optimal weights based on the query type
     * @param queryType as an option of three possibilities: 0 - memory-heavy, 1 - cpu-heavy, 2 - network-heavy
     * @param ftType level of reliability specified by the user
     */
    void adjustWeights(QueryType::Value queryType, FaultToleranceType::Value ftType);


    /**
     * Check if operator present in the given collection
     * @param operatorToSearch : operator to search
     * @param operatorCollection : collection to search into
     * @return true if successful
     */
    bool operatorPresentInCollection(const OperatorNodePtr& operatorToSearch,
                                     const std::vector<OperatorNodePtr>& operatorCollection);

    /**
 * @brief finds network, memory, safety available per path
 * @param path: nodes for placement
 * @param originalPath: original path
 * @return max and min available network, memory and safety on one path
 */
    ResourcesPerPath findResourcesAvailable(const std::vector<TopologyNodePtr> path, const std::vector<TopologyNodePtr> originalPath);

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
    bool isSourceAndDestinationConnected(const OperatorNodePtr& upStreamOperator, const OperatorNodePtr& downStreamOperator);
};
}// namespace NES::Optimizer
#endif// NES_CORE_INCLUDE_OPTIMIZER_QUERYPLACEMENT_BASEPLACEMENTSTRATEGY_HPP_

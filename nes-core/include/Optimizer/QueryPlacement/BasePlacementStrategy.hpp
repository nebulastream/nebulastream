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

#ifndef NES_INCLUDE_OPTIMIZER_QUERYPLACEMENT_BASEPLACEMENTSTRATEGY_HPP_
#define NES_INCLUDE_OPTIMIZER_QUERYPLACEMENT_BASEPLACEMENTSTRATEGY_HPP_

#include <Catalogs/Source/SourceCatalogEntry.hpp>
#include <Operators/OperatorId.hpp>
#include <Plans/Utils/PlanIdGenerator.hpp>
#include <Util/FaultToleranceType.hpp>
#include <Util/LineageType.hpp>
#include <chrono>
#include <iostream>
#include <map>
#include <memory>
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

class SourceCatalog;
using SourceCatalogPtr = std::shared_ptr<SourceCatalog>;

class GlobalExecutionPlan;
using GlobalExecutionPlanPtr = std::shared_ptr<GlobalExecutionPlan>;

class OperatorNode;
using OperatorNodePtr = std::shared_ptr<OperatorNode>;

class SourceLogicalOperatorNode;
using SourceLogicalOperatorNodePtr = std::shared_ptr<SourceLogicalOperatorNode>;

class NetworkSinkDescriptor;
using NetworkSinkDescriptorPtr = std::shared_ptr<NetworkSinkDescriptor>;
}// namespace NES

namespace NES::Optimizer {

class TypeInferencePhase;
using TypeInferencePhasePtr = std::shared_ptr<TypeInferencePhase>;

class BasePlacementStrategy;
using BasePlacementStrategyPtr = std::unique_ptr<BasePlacementStrategy>;

using PlacementMatrix = std::vector<std::vector<bool>>;

const std::string PINNED_NODE_ID = "PINNED_NODE_ID";
const std::string PLACED = "PLACED";

/**
 * @brief: This is the interface for base optimizer that needed to be implemented by any new query optimizer.
 */
class BasePlacementStrategy {

  private:
    static constexpr auto SINK_RETRIES = 100;
    static constexpr auto SINK_RETRY_WAIT = std::chrono::milliseconds(5);
    static constexpr auto SOURCE_RETRIES = 100;
    static constexpr auto SOURCE_RETRY_WAIT = std::chrono::milliseconds(5);

  public:
    explicit BasePlacementStrategy(GlobalExecutionPlanPtr globalExecutionPlan,
                                   TopologyPtr topologyPtr,
                                   TypeInferencePhasePtr typeInferencePhase,
                                   SourceCatalogPtr streamCatalog);

    virtual ~BasePlacementStrategy() = default;

    /**
     * Update Global Execution plan by placing the input query plan
     * @param queryPlan: the query plan to place
     * @return true if successful else false
     * @throws QueryPlacementException
     */
    virtual bool updateGlobalExecutionPlan(QueryPlanPtr queryPlan) = 0;

    /**
     *
     * @param pinnedUpstreamNodes
     * @return
     */
    virtual bool updateGlobalExecutionPlan(const std::vector<OperatorNodePtr>& pinnedUpstreamNodes) = 0;

    /**
     * @brief Modifies the execution graph of the query based on the newly added operators
     * @param queryPlan: the query plan
     * @return true if successful
     */
    virtual bool partiallyUpdateGlobalExecutionPlan(const QueryPlanPtr& queryPlan) = 0;

  protected:
    /**
     * @brief Map the logical source name to the physical source nodes in the topology used for placing the operators
     * @param sourceOperators: the source operators in the query
     */
    void mapPinnedOperatorToTopologyNodes(const QueryPlanPtr& queryPlan);

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
    void addNetworkSourceAndSinkOperators(const QueryPlanPtr& queryPlan);

    /**
     * @brief Run the type inference phase for all the query sub plans for the input query id
     * @param queryId: the input query id
     * @param faultToleranceType: fault-tolerance type
     * @param lineageType: lineage type
     * @return true if successful else false
     */
    bool runTypeInferencePhase(QueryId queryId, FaultToleranceType faultToleranceType, LineageType lineageType);

    /**
     * @brief assign binary matrix representation of placement mapping to the topology
     * @param topologyPtr the topology to place the operators
     * @param queryPlan query plan to place
     * @param mapping binary mapping determining whether to place an operator to a node
     */
    bool
    assignMappingToTopology(const NES::TopologyPtr topologyPtr, const NES::QueryPlanPtr queryPlan, const PlacementMatrix mapping);

    GlobalExecutionPlanPtr globalExecutionPlan;
    TopologyPtr topology;
    TypeInferencePhasePtr typeInferencePhase;
    SourceCatalogPtr streamCatalog;
    std::map<uint64_t, TopologyNodePtr> pinnedOperatorLocationMap;
    std::map<uint64_t, TopologyNodePtr> nodeIdToTopologyNodeMap;
    std::map<uint64_t, ExecutionNodePtr> operatorToExecutionNodeMap;
    std::unordered_map<OperatorId, QueryPlanPtr> operatorToSubPlan;

  private:
    /**
     * @brief create a new network sink operator
     * @param queryId : the query id to which the sink belongs to
     * @param sourceOperatorId : the operator id of the corresponding source operator
     * @param sourceTopologyNode : the topology node to which sink operator will send the data
     * @return the instance of network sink operator
     */
    static OperatorNodePtr
    createNetworkSinkOperator(QueryId queryId, uint64_t sourceOperatorId, const TopologyNodePtr& sourceTopologyNode);

    /**
     * @brief create a new network source operator
     * @param queryId : the query id to which the source belongs to
     * @param inputSchema : the schema for input event stream
     * @param operatorId : the operator id of the source network operator
     * @param sinkTopologyNode
     * @return the instance of network source operator
     */
    static OperatorNodePtr createNetworkSourceOperator(QueryId queryId,
                                                       SchemaPtr inputSchema,
                                                       uint64_t operatorId,
                                                       const TopologyNodePtr& sinkTopologyNode);

    /**
     * @brief Attach network source or sink operator to the given operator
     * @param queryId : the id of the query
     * @param operatorNode : the logical operator to which source or sink operator need to be attached
     */
    void placeNetworkOperator(QueryId queryId, const OperatorNodePtr& operatorNode);

    /**
     * @brief Add an execution node as root
     * @param executionNode
     */
    void addExecutionNodeAsRoot(ExecutionNodePtr& executionNode);

    /**
     * @brief Validates if the source operator and destination operator are connected directly or via network sinks
     * @param source : The source operator
     * @param destination : The destination operator
     * @return true if the source and destination are connected otherwise false
     */
    bool isSourceAndDestinationConnected(const OperatorNodePtr& source, const OperatorNodePtr& destination);
};
}// namespace NES::Optimizer
#endif// NES_INCLUDE_OPTIMIZER_QUERYPLACEMENT_BASEPLACEMENTSTRATEGY_HPP_

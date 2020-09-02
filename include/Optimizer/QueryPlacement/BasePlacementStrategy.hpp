#ifndef NESPLACEMENTOPTIMIZER_HPP
#define NESPLACEMENTOPTIMIZER_HPP

#include <API/QueryId.hpp>
#include <Catalogs/StreamCatalogEntry.hpp>
#include <chrono>
#include <iostream>
#include <map>
#include <memory>
#include <vector>

namespace NES {

class NESExecutionPlan;
typedef std::shared_ptr<NESExecutionPlan> NESExecutionPlanPtr;

class ExecutionNode;
typedef std::shared_ptr<ExecutionNode> ExecutionNodePtr;

class QueryPlan;
typedef std::shared_ptr<QueryPlan> QueryPlanPtr;

class Topology;
typedef std::shared_ptr<Topology> TopologyPtr;

class Schema;
typedef std::shared_ptr<Schema> SchemaPtr;

class NESTopologyEntry;
typedef std::shared_ptr<NESTopologyEntry> NESTopologyEntryPtr;

class NESTopologyGraph;
typedef std::shared_ptr<NESTopologyGraph> NESTopologyGraphPtr;

class LogicalOperatorNode;
typedef std::shared_ptr<LogicalOperatorNode> LogicalOperatorNodePtr;

class StreamCatalog;
typedef std::shared_ptr<StreamCatalog> StreamCatalogPtr;

class GlobalExecutionPlan;
typedef std::shared_ptr<GlobalExecutionPlan> GlobalExecutionPlanPtr;

class TypeInferencePhase;
typedef std::shared_ptr<TypeInferencePhase> TypeInferencePhasePtr;

class OperatorNode;
typedef std::shared_ptr<OperatorNode> OperatorNodePtr;

class SourceLogicalOperatorNode;
typedef std::shared_ptr<SourceLogicalOperatorNode> SourceLogicalOperatorNodePtr;

/**
 * @brief: This is the interface for base optimizer that needed to be implemented by any new query optimizer.
 */
class BasePlacementStrategy {

  private:
    static constexpr auto NSINK_RETRIES = 3;
    static constexpr auto NSINK_RETRY_WAIT = std::chrono::seconds(5);

  public:
    explicit BasePlacementStrategy(GlobalExecutionPlanPtr globalExecutionPlan, TopologyPtr topologyPtr, TypeInferencePhasePtr typeInferencePhase,
                                   StreamCatalogPtr streamCatalog);

    /**
     * @brief Returns an execution graph based on the input query and nes topology.
     * @param queryPlan: the query plan
     * @return true if successful
     */
    virtual bool updateGlobalExecutionPlan(QueryPlanPtr queryPlan) = 0;

  protected:
    /**
     * @brief Map the logical source name to the physical source nodes in the topology used for placing the operators
     * @param queryId : the id of the query.
     * @param sourceOperators: the source operators in the query
     */
    void mapPinnedOperatorToTopologyNodes(QueryId queryId, std::vector<SourceLogicalOperatorNodePtr> sourceOperators);

    /**
     * @brief Get Execution node for the input topology node
     * @param candidateTopologyNode: topology node
     * @return Execution Node pointer
     */
    ExecutionNodePtr getCandidateExecutionNode(TopologyNodePtr candidateTopologyNode);

    /**
     * @brief Get the physical node for the logical source
     * @param operatorId: the id of the operator
     * @return Topology node ptr or nullptr
     */
    TopologyNodePtr getTopologyNodeForPinnedOperator(uint64_t operatorId);

    /**
     * @brief Add network source and sinks between query sub plans allocated on different execution nodes
     * @param queryPlan: the original query plan
     */
    void addSystemGeneratedOperators(QueryPlanPtr queryPlan);

    /**
     * @brief Run the type inference phase for all the query sub plans for the input query id
     * @param queryId: the input query id
     */
    void runTypeInferencePhase(QueryId queryId);

    GlobalExecutionPlanPtr globalExecutionPlan;
    TopologyPtr topology;
    TypeInferencePhasePtr typeInferencePhase;
    StreamCatalogPtr streamCatalog;
    std::map<uint64_t, TopologyNodePtr> pinnedOperatorLocationMap;
    std::map<uint64_t, ExecutionNodePtr> operatorToExecutionNodeMap;
  private:

    /**
     * @brief create a new network sink operator
     * @param queryId : the query id to which the sink belongs to
     * @param sourceOperatorId : the operator id of the corresponding source operator
     * @param sourceTopologyNode : the topology node to which sink operator will send the data
     * @return the instance of network sink operator
     */
    OperatorNodePtr createNetworkSinkOperator(QueryId queryId, uint64_t sourceOperatorId, TopologyNodePtr sourceTopologyNode);

    /**
     * @brief create a new network source operator
     * @param queryId : the query id to which the source belongs to
     * @param inputSchema : the schema for input event stream
     * @param operatorId : the operator id of the source network operator
     * @return the instance of network source operator
     */
    OperatorNodePtr createNetworkSourceOperator(QueryId queryId, SchemaPtr inputSchema, uint64_t operatorId);

    /**
     * @brief Attach network source or sink operator to the given operator
     * @param queryId : the id of the query
     * @param operatorNode : the logical operator to which source or sink operator need to be attached
     */
    void placeNetworkOperator(QueryId queryId, OperatorNodePtr operatorNode);
};
}// namespace NES
#endif//NESPLACEMENTOPTIMIZER_HPP

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
    void mapLogicalSourceToTopologyNodes(QueryId queryId, std::vector<SourceLogicalOperatorNodePtr> sourceOperators);

    /**
     * @brief Add a system generated network sink operator to the input query plan
     * @param queryPlan
     * @param parentNesNode
     * @return returns the operator id of the next network source operator
     */
    uint64_t addNetworkSinkOperator(QueryPlanPtr queryPlan, TopologyNodePtr parentNesNode);

    /**
     * @brief Add a system generated network source operator to the input query plan.
     * @param queryPlan: the query plan to which network source operator is to be added.
     * @param inputSchema: The schema of the data to be received from upstream sink operator.
     * @param operatorId: The id of the network source operator.
     */
    void addNetworkSourceOperator(QueryPlanPtr queryPlan, SchemaPtr inputSchema, uint64_t operatorId);

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

    GlobalExecutionPlanPtr globalExecutionPlan;
    TopologyPtr topology;
    TypeInferencePhasePtr typeInferencePhase;
    StreamCatalogPtr streamCatalog;
    std::map<uint64_t, TopologyNodePtr> pinnedOperatorLocationMap;
};
}// namespace NES
#endif//NESPLACEMENTOPTIMIZER_HPP

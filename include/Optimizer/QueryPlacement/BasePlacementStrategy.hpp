#ifndef NESPLACEMENTOPTIMIZER_HPP
#define NESPLACEMENTOPTIMIZER_HPP

#include <iostream>
#include <map>
#include <memory>
#include <vector>
#include <chrono>

namespace NES {

class NESExecutionPlan;
typedef std::shared_ptr<NESExecutionPlan> NESExecutionPlanPtr;

class ExecutionNode;
typedef std::shared_ptr<ExecutionNode> ExecutionNodePtr;

class QueryPlan;
typedef std::shared_ptr<QueryPlan> QueryPlanPtr;

class NESTopologyPlan;
typedef std::shared_ptr<NESTopologyPlan> NESTopologyPlanPtr;

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

class PathFinder;
typedef std::shared_ptr<PathFinder> PathFinderPtr;

class GlobalExecutionPlan;
typedef std::shared_ptr<GlobalExecutionPlan> GlobalExecutionPlanPtr;

class TypeInferencePhase;
typedef std::shared_ptr<TypeInferencePhase> TypeInferencePhasePtr;

class OperatorNode;
typedef std::shared_ptr<OperatorNode> OperatorNodePtr;

/**
 * @brief: This is the interface for base optimizer that needed to be implemented by any new query optimizer.
 */
class BasePlacementStrategy {

  private:
    static constexpr auto NSINK_RETRIES = 3;
    static constexpr auto NSINK_RETRY_WAIT = std::chrono::seconds(5);
    static constexpr auto ZMQ_DEFAULT_PORT = 5555;

  public:
    explicit BasePlacementStrategy(GlobalExecutionPlanPtr globalExecutionPlan, NESTopologyPlanPtr nesTopologyPlan, TypeInferencePhasePtr typeInferencePhase,
                                   StreamCatalogPtr streamCatalog);

    /**
     * @brief Returns an execution graph based on the input query and nes topology.
     * @param queryPlan: the query plan
     * @return true if successful
     */
    virtual bool updateGlobalExecutionPlan(QueryPlanPtr queryPlan) = 0;

  private:
    /**
     * @brief Add a system generated network sink operator to the input query plan
     * @param queryPlan
     * @param parentNesNode
     * @return returns the operator id of the next network source operator
     */
    uint64_t addNetworkSinkOperator(QueryPlanPtr queryPlan, NESTopologyEntryPtr parentNesNode);

    /**
     * @brief Add a system generated network source operator to the input query plan.
     * @param queryPlan: the query plan to which network source operator is to be added.
     * @param childNesNode: The child nes node where the corresponding upstream network sink operator is located.
     * @param operatorId: The id of the network source operator.
     */
    void addNetworkSourceOperator(QueryPlanPtr queryPlan, NESTopologyEntryPtr childNesNode, uint64_t operatorId);

  protected:
    /**
     * @brief This method will add the system generated operators where ever necessary along the selected path for operator placement.
     *
     * @param queryId query Id of the sub plan for which the operators have to be placed
     * @param path vector of nodes where operators could be placed
     */
    void addSystemGeneratedOperators(uint64_t queryId, std::vector<NESTopologyEntryPtr> path);

    GlobalExecutionPlanPtr globalExecutionPlan;
    NESTopologyPlanPtr nesTopologyPlan;
    TypeInferencePhasePtr typeInferencePhase;
    StreamCatalogPtr streamCatalog;
    PathFinderPtr pathFinder;
};
}// namespace NES
#endif//NESPLACEMENTOPTIMIZER_HPP

#ifndef NESPLACEMENTOPTIMIZER_HPP
#define NESPLACEMENTOPTIMIZER_HPP

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
    static const int ZMQ_DEFAULT_PORT = 5555;

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
     * Create a new system sink operator
     * @param nesNode
     * @return A logical system generated sink operator
     */
    OperatorNodePtr createNetworkSinkOperator(NESTopologyEntryPtr nesNode);

    /**
     * Create a new system source operator
     * @param nesNode
     * @param schema
     * @return A logical system generated source operator
     */
    OperatorNodePtr createNetworkSourceOperator(NESTopologyEntryPtr nesNode, SchemaPtr schema);

    /**
     * @brief
     * @param queryPlan
     * @param parentNesNode
     */
    void addNetworkSinkOperator(QueryPlanPtr queryPlan, NESTopologyEntryPtr parentNesNode);

    /**
     * @brief
     * @param queryPlan
     * @param currentNesNode
     * @param childNesNode
     */
    void addNetworkSourceOperator(QueryPlanPtr queryPlan, NESTopologyEntryPtr currentNesNode, NESTopologyEntryPtr childNesNode);

  protected:
    /**
     * @brief This method will add the system generated operators where ever necessary along the selected path for operator placement.
     *
     * @param queryId query Id of the sub plan for which the operators have to be placed
     * @param path vector of nodes where operators could be placed
     */
    void addSystemGeneratedOperators(std::string queryId, std::vector<NESTopologyEntryPtr> path);

    GlobalExecutionPlanPtr globalExecutionPlan;
    NESTopologyPlanPtr nesTopologyPlan;
    TypeInferencePhasePtr typeInferencePhase;
    StreamCatalogPtr streamCatalog;
    PathFinderPtr pathFinder;
};
}// namespace NES
#endif//NESPLACEMENTOPTIMIZER_HPP

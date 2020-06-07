#ifndef NESPLACEMENTOPTIMIZER_HPP
#define NESPLACEMENTOPTIMIZER_HPP

#include <iostream>
#include <map>
#include <memory>
#include <vector>

namespace NES {

enum NESPlacementStrategyType {
    TopDown,
    BottomUp,
    // FIXME: enable them with issue #755
    LowLatency,
    HighThroughput,
    MinimumResourceConsumption,
    MinimumEnergyConsumption,
    HighAvailability
};

static std::map<std::string, NESPlacementStrategyType> stringToPlacementStrategyType{
    {"BottomUp", BottomUp},
    {"TopDown", TopDown},
    // FIXME: enable them with issue #755
//    {"Latency", LowLatency},
//    {"HighThroughput", HighThroughput},
//    {"MinimumResourceConsumption", MinimumResourceConsumption},
//    {"MinimumEnergyConsumption", MinimumEnergyConsumption},
//    {"HighAvailability", HighAvailability},
};

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

class Operator;
typedef std::shared_ptr<Operator> OperatorPtr;

class StreamCatalog;
typedef std::shared_ptr<StreamCatalog> StreamCatalogPtr;

class PathFinder;
typedef std::shared_ptr<PathFinder> PathFinderPtr;

class GlobalExecutionPlan;
typedef std::shared_ptr<GlobalExecutionPlan> GlobalExecutionPlanPtr;

/**
 * @brief: This is the interface for base optimizer that needed to be implemented by any new query optimizer.
 */
class BasePlacementStrategy {

  private:
    static const int zmqDefaultPort = 5555;

  public:

    explicit BasePlacementStrategy(NESTopologyPlanPtr nesTopologyPlan);

    /**
     * @brief Factory method returning different kind of optimizer.
     * @param nesTopologyPlan topology information
     * @param placementStrategyName name of the strategy
     * @return instance of type BaseOptimizer
     */
    static std::unique_ptr<BasePlacementStrategy> getStrategy(NESTopologyPlanPtr nesTopologyPlan, std::string placementStrategyName);

    /**
     * @brief Returns an execution graph based on the input query and nes topology.
     * @param queryPlan
     * @param nesTopologyPlan
     * @return
     */
    virtual GlobalExecutionPlanPtr initializeExecutionPlan(QueryPlanPtr queryPlan, StreamCatalogPtr streamCatalog) = 0;

  private:

    void createExecutionNodeWithForwardOperators();

    OperatorNodePtr createSystemSinkOperator(NESTopologyEntryPtr nesNode);
    OperatorNodePtr createSystemSourceOperator(NESTopologyEntryPtr nesNode, SchemaPtr schema);

  protected:

    /**
     * @brief This method will add the system generated operators where ever necessary along the selected path for operator placement.
     *
     * @param queryId query Id of the sub plan for which the operators have to be placed
     * @param path vector of nodes where operators could be placed
     * @param executionPlan Pointer to the execution plan
     */
    void addSystemGeneratedOperators(std::string queryId, std::vector<NESTopologyEntryPtr> path, GlobalExecutionPlanPtr executionPlan);

    NESTopologyPlanPtr nesTopologyPlan;
    PathFinderPtr pathFinder;
};
}// namespace NES
#endif//NESPLACEMENTOPTIMIZER_HPP

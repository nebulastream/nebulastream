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
    LowLatency,
    HighThroughput,
    MinimumResourceConsumption,
    MinimumEnergyConsumption,
    HighAvailability
};

static std::map<std::string, NESPlacementStrategyType> stringToPlacementStrategyType{
    {"BottomUp", BottomUp},
    {"TopDown", TopDown},
    {"Latency", LowLatency},
    {"HighThroughput", HighThroughput},
    {"MinimumResourceConsumption", MinimumResourceConsumption},
    {"MinimumEnergyConsumption", MinimumEnergyConsumption},
    {"HighAvailability", HighAvailability},
};

class NESExecutionPlan;
typedef std::shared_ptr<NESExecutionPlan> NESExecutionPlanPtr;

class ExecutionNode;
typedef std::shared_ptr<ExecutionNode> ExecutionNodePtr;

class Query;
typedef std::shared_ptr<Query> QueryPtr;

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

/**
 * @brief: This is the interface for base optimizer that needed to be implemented by any new query optimizer.
 */
class BasePlacementStrategy {

  private:
    const char* NO_OPERATOR = "NO-OPERATOR";

  public:
    BasePlacementStrategy(){};

    /**
     * @brief Returns an execution graph based on the input query and nes topology.
     * @param inputQuery
     * @param nesTopologyPlan
     * @return
     */
    virtual NESExecutionPlanPtr initializeExecutionPlan(QueryPtr inputQuery, NESTopologyPlanPtr nesTopologyPlan) = 0;

    /**
     * @brief This method will add system generated zmq source and sinks for each execution node.
     * @note We use zmq for internal message transfer therefore the source and sink will be zmq based.
     * @note This method will not append zmq source or sink if the operator chain in an execution node already contains
     * user defined source or sink operator respectively.
     *
     * @param schema
     * @param nesExecutionPlanPtr
     */
    void addSystemGeneratedSourceSinkOperators(SchemaPtr schema, NESExecutionPlanPtr nesExecutionPlanPtr);

    /**
     * @brief Fill the execution graph with complete topology information and assign No-Operators to the node which were
     * not selected for operator placement by the placement strategy.
     *
     * Note: This method is necessary for displaying the execution graph on NES-UI
     *
     * @param nesExecutionPlanPtr
     * @param nesTopologyPtr
     */
    void fillExecutionGraphWithTopologyInformation(NESExecutionPlanPtr nesExecutionPlanPtr,
                                                   NESTopologyPlanPtr nesTopologyPtr);

    /**
     * @brief Factory method returning different kind of optimizer.
     * @param placementStrategyName
     * @return instance of type BaseOptimizer
     */
    static std::unique_ptr<BasePlacementStrategy> getStrategy(std::string placementStrategyName);

    /**
     * @brief replace forward operator with system generated source and sink operator.
     * @param schema schema of the incoming or outgoing data for source and sink operator.
     * @param executionNodePtr information about the execution node
     */
    void convertFwdOptr(SchemaPtr schema, ExecutionNodePtr executionNodePtr) const;

    /**
     * @brief This method will add the forward operator where ever necessary along the selected path.
     *
     * @param candidateNodes vector of nodes where operators could be placed
     * @param nesExecutionPlanPtr Pointer to the execution plan
     */
    void addForwardOperators(std::vector<NESTopologyEntryPtr> candidateNodes, NESExecutionPlanPtr nesExecutionPlanPtr);

  protected:
    StreamCatalogPtr streamCatalog;
};
}// namespace NES
#endif//NESPLACEMENTOPTIMIZER_HPP

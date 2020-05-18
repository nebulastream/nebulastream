#include <API/Query.hpp>
#include <Nodes/Operators/OperatorNode.hpp>
#include <Nodes/Operators/QueryPlan.hpp>
#include <Nodes/Phases/TranslateFromLegacyPlanPhase.hpp>
#include <Optimizer/NESOptimizer.hpp>
#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <Topology/NESTopologyPlan.hpp>
#include <Util/Logger.hpp>

using namespace NES;

NESOptimizer::NESOptimizer() {
    translateFromLegacyPlanPhase = TranslateFromLegacyPlanPhase::create();
}

NESExecutionPlanPtr NESOptimizer::prepareExecutionGraph(std::string strategy, InputQueryPtr inputQuery,
                                                        NESTopologyPlanPtr nesTopologyPlan) {

    NES_INFO("NESOptimizer: Preparing execution graph for input query");

    NES_INFO("NESOptimizer: Initializing Placement strategy");
    auto placementStrategyPtr = BasePlacementStrategy::getStrategy(strategy);

    NES_INFO("NESOptimizer: Converting Legacy InputQuery into new Query");
    const OperatorPtr rootOperator = inputQuery->getRoot();
    NES_INFO("NESOptimizer: Transforming old operator graph into new OperatorNode graph");
    const OperatorNodePtr rootNodeOperator = translateFromLegacyPlanPhase->transform(rootOperator);
    auto rootOperatorId = rootNodeOperator->getId();

    NES_INFO("NESOptimizer: Creating QueryPlan");
    auto queryPlan = QueryPlan::create(rootNodeOperator);
    //FIXME: (TEMP) This is a temporary hack to reset the operator ID of root operator after building the QueryPlan
    queryPlan->getRootOperator()->setId(rootOperatorId);
    NES_INFO("NESOptimizer: Building Query");
    const std::string sourceStreamName = inputQuery->getSourceStream()->getName();
    Query query = Query::createFromQueryPlan(sourceStreamName, queryPlan);
    QueryPtr queryPtr = std::make_shared<Query>(query);

    NES_INFO("NESOptimizer: Building Execution plan for the input query");
    NESExecutionPlanPtr nesExecutionPlanPtr = placementStrategyPtr->initializeExecutionPlan(queryPtr, nesTopologyPlan);
    return nesExecutionPlanPtr;
};

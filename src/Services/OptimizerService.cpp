#include <Nodes/Phases/TypeInferencePhase.hpp>
#include <Operators/OperatorJsonUtil.hpp>
#include <Optimizer/NESOptimizer.hpp>
#include <Services/OptimizerService.hpp>
#include <Topology/TopologyManager.hpp>
#include <Util/Logger.hpp>
#include <chrono>

using namespace NES;
using namespace web;
using namespace std;
using namespace std::chrono;

OptimizerService::OptimizerService(TopologyManagerPtr topologyManager, GlobalExecutionPlan) : topologyManager(topologyManager) {
    NES_DEBUG("OptimizerService()");
}

json::value OptimizerService::getExecutionPlanAsJson(QueryPlanPtr queryPlan, string optimizationStrategyName, StreamCatalogPtr streamCatalog) {
    return getExecutionPlan(queryPlan, optimizationStrategyName, streamCatalog)->getExecutionGraphAsJson();
}

NESExecutionPlanPtr OptimizerService::getExecutionPlan(QueryPlanPtr queryPlan, string optimizationStrategyName, StreamCatalogPtr streamCatalog) {

    TypeInferencePhasePtr typeInferencePhasePtr = TypeInferencePhase::create();
    typeInferencePhasePtr->setStreamCatalog(streamCatalog);
    queryPlan = typeInferencePhasePtr->transform(queryPlan);

    const NESTopologyPlanPtr topologyPlan = topologyManager->getNESTopologyPlan();
    NES_DEBUG("OptimizerService: topology=" << topologyPlan->getTopologyPlanString());

    NESOptimizer queryOptimizer;

    OperatorJsonUtil operatorJsonUtil;
    const json::value& basePlan = operatorJsonUtil.getBasePlan(queryPlan);

    NES_DEBUG("OptimizerService: query plan=" << basePlan);
    const GlobalExecutionPlanPtr executionGraph = queryOptimizer.prepareExecutionGraph(optimizationStrategyName, inputQuery, topologyPlan, inputQuery->streamCatalog);
    return executionGraph;
}

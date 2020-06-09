#include <Nodes/Phases/TypeInferencePhase.hpp>
#include <Operators/OperatorJsonUtil.hpp>
#include <Optimizer/NESOptimizer.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Services/OptimizerService.hpp>
#include <Topology/TopologyManager.hpp>
#include <Util/Logger.hpp>
#include <chrono>

using namespace NES;
using namespace web;

OptimizerService::OptimizerService(TopologyManagerPtr topologyManager, StreamCatalogPtr streamCatalog, GlobalExecutionPlanPtr globalExecutionPlan)
    : topologyManager(topologyManager), streamCatalog(streamCatalog), globalExecutionPlan(globalExecutionPlan) {
    NES_DEBUG("OptimizerService()");
}

std::string OptimizerService::getExecutionPlanAsString(QueryPlanPtr queryPlan, string optimizationStrategyName) {
    return updateGlobalExecutionPlan(queryPlan, optimizationStrategyName)->getAsString();
}

GlobalExecutionPlanPtr OptimizerService::updateGlobalExecutionPlan(QueryPlanPtr queryPlan, string optimizationStrategyName) {

    TypeInferencePhasePtr typeInferencePhasePtr = TypeInferencePhase::create();
    typeInferencePhasePtr->setStreamCatalog(streamCatalog);
    queryPlan = typeInferencePhasePtr->transform(queryPlan);

    const NESTopologyPlanPtr topologyPlan = topologyManager->getNESTopologyPlan();
    NES_DEBUG("OptimizerService: topology=" << topologyPlan->getTopologyPlanString());

    NESOptimizer queryOptimizer;

    OperatorJsonUtil operatorJsonUtil;
    const json::value& basePlan = operatorJsonUtil.getBasePlan(queryPlan);

    NES_DEBUG("OptimizerService: query plan=" << basePlan);
    const GlobalExecutionPlanPtr executionGraph = queryOptimizer.updateExecutionGraph(optimizationStrategyName, queryPlan, topologyPlan, streamCatalog, globalExecutionPlan);
    return executionGraph;
}

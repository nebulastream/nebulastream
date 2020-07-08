#include <Phases/TypeInferencePhase.hpp>
#include <Operators/OperatorJsonUtil.hpp>
#include <Optimizer/NESOptimizer.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Services/OptimizerService.hpp>
#include <Topology/TopologyManager.hpp>
#include <Util/Logger.hpp>

using namespace web;

namespace NES{
OptimizerService::OptimizerService(TopologyManagerPtr topologyManager, StreamCatalogPtr streamCatalog, GlobalExecutionPlanPtr globalExecutionPlan)
    : topologyManager(topologyManager), streamCatalog(streamCatalog), globalExecutionPlan(globalExecutionPlan) {
    NES_DEBUG("OptimizerService()");
}

std::string OptimizerService::getExecutionPlanAsString(QueryPlanPtr queryPlan, string optimizationStrategyName) {
    return updateGlobalExecutionPlan(queryPlan, optimizationStrategyName)->getAsString();
}

GlobalExecutionPlanPtr OptimizerService::updateGlobalExecutionPlan(QueryPlanPtr queryPlan, string optimizationStrategyName) {

    TypeInferencePhasePtr typeInferencePhasePtr = TypeInferencePhase::create(streamCatalog);
    queryPlan = typeInferencePhasePtr->transform(queryPlan);

    const NESTopologyPlanPtr topologyPlan = topologyManager->getNESTopologyPlan();
    NES_DEBUG("OptimizerService: topology=" << topologyPlan->getTopologyPlanString());

    NESOptimizer queryOptimizer;
    return queryOptimizer.updateExecutionGraph(optimizationStrategyName, queryPlan, topologyPlan, streamCatalog, globalExecutionPlan);
}
}

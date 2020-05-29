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

OptimizerService::OptimizerService(TopologyManagerPtr topologyManager) : topologyManager(topologyManager) {
    NES_DEBUG("OptimizerService()");
}

json::value OptimizerService::getExecutionPlanAsJson(QueryPtr queryPtr, string optimizationStrategyName) {
    return getExecutionPlan(queryPtr, optimizationStrategyName)->getExecutionGraphAsJson();
}

NESExecutionPlanPtr OptimizerService::getExecutionPlan(QueryPtr queryPtr, string optimizationStrategyName) {

    const NESTopologyPlanPtr& topologyPlan = topologyManager->getNESTopologyPlan();
    NES_DEBUG("OptimizerService: topology=" << topologyPlan->getTopologyPlanString());

    NESOptimizer queryOptimizer;

    OperatorJsonUtil operatorJsonUtil;
    const json::value& basePlan = operatorJsonUtil.getBasePlan(queryPtr);

    NES_DEBUG("OptimizerService: query plan=" << basePlan);

    auto start = high_resolution_clock::now();

    const NESExecutionPlanPtr
        executionGraph = queryOptimizer.prepareExecutionGraph(optimizationStrategyName, queryPtr, topologyPlan, queryPtr->streamCatalog);

    auto stop = high_resolution_clock::now();
    const auto duration = duration_cast<milliseconds>(stop - start);
    long durationInMillis = duration.count();

    executionGraph->setTotalComputeTimeInMillis(durationInMillis);

    return executionGraph;
}

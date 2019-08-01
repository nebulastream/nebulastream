#include <Optimizer/FogPlacementOptimizer.hpp>
#include <Topology/FogTopologyPlan.hpp>
#include <Optimizer/FogOptimizer.hpp>
#include <Topology/FogTopologyManager.hpp>

using namespace iotdb;

FogExecutionPlan FogOptimizer::prepareExecutionGraph(std::string strategy, InputQuery inputQuery,
                                                     FogTopologyPlanPtr fogTopologyPlan) {

    FogPlacementOptimizer *pBaseOptimizer = FogPlacementOptimizer::getOptimizer(strategy);

    const FogExecutionPlan &executionGraph = pBaseOptimizer->initializeExecutionPlan(inputQuery, fogTopologyPlan);

    return executionGraph;

};

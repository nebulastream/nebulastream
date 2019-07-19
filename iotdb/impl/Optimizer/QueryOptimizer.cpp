#include <Optimizer/BaseOptimizer.hpp>
#include <Topology/FogTopologyPlan.hpp>
#include <Optimizer/QueryOptimizer.hpp>
#include <Topology/FogTopologyManager.hpp>

using namespace iotdb;

ExecutionGraph QueryOptimizer::prepareExecutionGraph(std::string strategy, InputQuery inputQuery,
                                                     FogTopologyPlanPtr fogTopologyPlan) {

    BaseOptimizer *pBaseOptimizer = BaseOptimizer::getOptimizer(strategy);

    const ExecutionGraph &executionGraph = pBaseOptimizer->prepareExecutionPlan(inputQuery, fogTopologyPlan);

    return executionGraph;

};

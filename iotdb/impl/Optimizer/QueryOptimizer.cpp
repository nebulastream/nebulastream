#include <Optimizer/BaseOptimizer.hpp>
#include <Topology/FogTopologyPlan.hpp>
#include <Optimizer/QueryOptimizer.hpp>

using namespace iotdb;

OptimizedExecutionGraph QueryOptimizer::prepareExecutionGraph(std::string strategy, InputQuery inputQuery,
                                                              FogTopologyPlan fogTopologyPlan) {

    BaseOptimizer *pBaseOptimizer = BaseOptimizer::getOptimizer(strategy);

};

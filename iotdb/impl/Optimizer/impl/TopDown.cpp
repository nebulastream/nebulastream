#include "Optimizer/impl/TopDown.hpp"

using namespace iotdb;

FogExecutionPlan TopDown::prepareExecutionPlan(iotdb::InputQuery inputQuery,
                                               iotdb::FogTopologyPlanPtr fogTopologyPlan) {
    FogExecutionPlan executionGraph;
    const OperatorPtr &sinkOperator = inputQuery.getRoot();


    placeOperators(executionGraph, inputQuery, fogTopologyPlan);

    completeExecutionGraphWithFogTopology(executionGraph, fogTopologyPlan);
    return executionGraph;

}

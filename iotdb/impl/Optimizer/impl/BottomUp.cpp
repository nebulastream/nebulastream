#include "Optimizer/impl/BottomUp.hpp"
#include <Operators/Operator.hpp>

using namespace iotdb;
using namespace std;

FogExecutionPlan BottomUp::initializeExecutionPlan(InputQuery inputQuery, FogTopologyPlanPtr fogTopologyPlan) {

    FogExecutionPlan executionGraph;
    const OperatorPtr &sinkOperator = inputQuery.getRoot();
    const vector<OperatorPtr> &sourceOperators = getSourceOperators(sinkOperator);
    const deque<FogTopologyEntryPtr> &sourceNodes = getSourceNodes(fogTopologyPlan);
    placeOperators(executionGraph, fogTopologyPlan, sourceOperators, sourceNodes);
    completeExecutionGraphWithFogTopology(executionGraph, fogTopologyPlan);
    return executionGraph;
}



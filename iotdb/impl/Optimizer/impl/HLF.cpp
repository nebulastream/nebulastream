#include "Optimizer/impl/HLF.hpp"
#include <Operators/Operator.hpp>

using namespace iotdb;
using namespace std;

ExecutionGraph HLF::prepareExecutionPlan(InputQuery inputQuery, FogTopologyPlanPtr fogTopologyPlan) {


    OptimizedExecutionGraph executionGraph;

    const OperatorPtr &sinkOperator = inputQuery.getRoot();
    const vector<OperatorPtr> &sourceOperators = getSourceOperators(sinkOperator);
    const deque<FogTopologyEntryPtr> &sourceNodes = getSourceNodes(fogTopologyPlan);

    placeOperators(executionGraph, fogTopologyPlan, sourceOperators, sourceNodes);
    executionGraph.getTopologyPlanString();

    return executionGraph.getExecutionGraph();
}



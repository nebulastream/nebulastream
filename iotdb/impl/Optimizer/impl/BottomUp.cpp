#include "Optimizer/impl/BottomUp.hpp"
#include <Operators/Operator.hpp>

using namespace iotdb;
using namespace std;

FogExecutionPlan BottomUp::initializeExecutionPlan(InputQuery inputQuery, FogTopologyPlanPtr fogTopologyPlan) {

  FogExecutionPlan executionGraph;
  const OperatorPtr &sinkOperator = inputQuery.getRoot();
  const string &streamName = inputQuery.source_stream.getName();
  const vector<OperatorPtr> &sourceOperators = getSourceOperators(sinkOperator);
  const deque<FogTopologyEntryPtr> &sourceNodes = getSourceNodes(fogTopologyPlan, streamName);

  if (sourceNodes.empty()) {
    std::cout << "Unable to find the target source";
    throw Exception("No source found in the topology");
  }

  placeOperators(executionGraph, fogTopologyPlan, sourceOperators, sourceNodes);
  completeExecutionGraphWithFogTopology(executionGraph, fogTopologyPlan);
  return executionGraph;
}



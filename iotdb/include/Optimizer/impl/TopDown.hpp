#ifndef IOTDB_TOPDOWN_HPP
#define IOTDB_TOPDOWN_HPP

#include <Optimizer/FogPlacementOptimizer.hpp>
#include <Operators/Operator.hpp>
#include <stack>

namespace iotdb {

using namespace std;

class TopDown : public FogPlacementOptimizer {

 public:
  FogExecutionPlan initializeExecutionPlan(InputQuery inputQuery, FogTopologyPlanPtr fogTopologyPlan);

 private:

  void placeOperators(FogExecutionPlan executionGraph, InputQuery query, FogTopologyPlanPtr fogTopologyPlanPtr);

};

}
#endif //IOTDB_TOPDOWN_HPP

#ifndef IOTDB_TOPDOWN_HPP
#define IOTDB_TOPDOWN_HPP

#include <Optimizer/FogPlacementOptimizer.hpp>
#include <Operators/Operator.hpp>
#include <stack>

namespace iotdb {

using namespace std;

class TopDown : public FogPlacementOptimizer {

 public:
  TopDown()= default;
  ~TopDown()= default;

  FogExecutionPlan initializeExecutionPlan(InputQueryPtr inputQuery, FogTopologyPlanPtr fogTopologyPlan) override;

 private:

  void placeOperators(FogExecutionPlan executionGraph, InputQueryPtr query, FogTopologyPlanPtr fogTopologyPlanPtr);

};

}
#endif //IOTDB_TOPDOWN_HPP

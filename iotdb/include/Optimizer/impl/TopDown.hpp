#ifndef IOTDB_TOPDOWN_HPP
#define IOTDB_TOPDOWN_HPP

#include <Operators/Operator.hpp>
#include <stack>
#include "../NESPlacementOptimizer.hpp"

namespace iotdb {

using namespace std;

class TopDown : public NESPlacementOptimizer {

 public:
  TopDown()= default;
  ~TopDown()= default;

  NESExecutionPlan initializeExecutionPlan(InputQueryPtr inputQuery, NESTopologyPlanPtr fogTopologyPlan) override;

 private:

  void placeOperators(NESExecutionPlan executionGraph, InputQueryPtr query, NESTopologyPlanPtr fogTopologyPlanPtr);

};

}
#endif //IOTDB_TOPDOWN_HPP

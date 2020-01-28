#ifndef TOPDOWN_HPP
#define TOPDOWN_HPP

#include <Operators/Operator.hpp>
#include <stack>
#include "../NESPlacementOptimizer.hpp"

namespace NES {

using namespace std;

class TopDown : public NESPlacementOptimizer {

  public:
    TopDown() = default;
    ~TopDown() = default;

    NESExecutionPlan initializeExecutionPlan(InputQueryPtr inputQuery, NESTopologyPlanPtr nesTopologyPlanPtr) override;

  private:

    void placeOperators(NESExecutionPlan executionGraph, const OperatorPtr& sinkOperator,
                        deque<NESTopologyEntryPtr> sourceNodes, const NESTopologyGraphPtr& nesGraphPtr);

};

}
#endif //TOPDOWN_HPP

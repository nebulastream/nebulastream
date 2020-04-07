#ifndef NES_IMPL_OPTIMIZER_IMPL_HIGHTHROUGHPUT_HPP_
#define NES_IMPL_OPTIMIZER_IMPL_HIGHTHROUGHPUT_HPP_

#include <Optimizer/NESPlacementOptimizer.hpp>

namespace NES {

/**
 * @brief This class is responsible for placing operators on high capacity links such that the overall query throughput
 * will increase.
 */
class HighThroughputStrategy : public NESPlacementOptimizer {

  public:
    HighThroughputStrategy() = default;
    ~HighThroughputStrategy() = default;

    NESExecutionPlanPtr initializeExecutionPlan(InputQueryPtr inputQuery, NESTopologyPlanPtr nesTopologyPlan);

  private:

    void placeOperators(NESExecutionPlanPtr executionPlanPtr, NESTopologyGraphPtr nesTopologyGraphPtr,
                        OperatorPtr operatorPtr, vector<NESTopologyEntryPtr> sourceNodes);

    NESPlacementStrategyType getType() {
        return HighThroughput;
    }

};

}

#endif //NES_IMPL_OPTIMIZER_IMPL_HIGHTHROUGHPUT_HPP_

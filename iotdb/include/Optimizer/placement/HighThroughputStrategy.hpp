#ifndef NES_IMPL_OPTIMIZER_IMPL_HIGHTHROUGHPUT_HPP_
#define NES_IMPL_OPTIMIZER_IMPL_HIGHTHROUGHPUT_HPP_

#include <Optimizer/BasePlacementStrategy.hpp>

namespace NES {

/**
 * @brief This class is responsible for placing operators on high capacity links such that the overall query throughput
 * will increase.
 */
class HighThroughputStrategy : public BasePlacementStrategy {

  public:
    HighThroughputStrategy() = default;
    ~HighThroughputStrategy() = default;

    NESExecutionPlanPtr initializeExecutionPlan(InputQueryPtr inputQuery, NESTopologyPlanPtr nesTopologyPlan);

    NESPlacementStrategyType getType() override;

  private:

    void placeOperators(NESExecutionPlanPtr executionPlanPtr, NESTopologyGraphPtr nesTopologyGraphPtr,
                        OperatorPtr operatorPtr, vector<NESTopologyEntryPtr> sourceNodes);

};

}

#endif //NES_IMPL_OPTIMIZER_IMPL_HIGHTHROUGHPUT_HPP_

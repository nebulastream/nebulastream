#ifndef NES_IMPL_OPTIMIZER_IMPL_MINIMUMENERGYCONSUMPTION_HPP_
#define NES_IMPL_OPTIMIZER_IMPL_MINIMUMENERGYCONSUMPTION_HPP_

#include <Optimizer/BasePlacementStrategy.hpp>
namespace NES {

/**
 * @brief This class is responsible for placing the operators on common path among the sources such that overall
 * energy consumption will reduce.
 */
class MinimumEnergyConsumptionStrategy : public BasePlacementStrategy {

  public:
    MinimumEnergyConsumptionStrategy() = default;
    ~MinimumEnergyConsumptionStrategy() = default;

    NESExecutionPlanPtr initializeExecutionPlan(InputQueryPtr inputQuery, NESTopologyPlanPtr nesTopologyPlan);

    NESPlacementStrategyType getType() override;
  private:

    void placeOperators(NESExecutionPlanPtr executionPlanPtr, NESTopologyGraphPtr nesTopologyGraphPtr,
                        OperatorPtr sourceOperator, vector<NESTopologyEntryPtr> sourceNodes);
};
}

#endif //NES_IMPL_OPTIMIZER_IMPL_MINIMUMENERGYCONSUMPTION_HPP_

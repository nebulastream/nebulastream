#ifndef NES_IMPL_OPTIMIZER_IMPL_MINIMUMENERGYCONSUMPTION_HPP_
#define NES_IMPL_OPTIMIZER_IMPL_MINIMUMENERGYCONSUMPTION_HPP_

#include <Optimizer/NESPlacementOptimizer.hpp>
namespace NES {

/**
 * @brief This class is responsible for placing the operators on common path among the sources such that overall
 * energy consumption will reduce.
 */
class MinimumEnergyConsumptionStrategy : public NESPlacementOptimizer {

  public:
    MinimumEnergyConsumptionStrategy() = default;
    ~MinimumEnergyConsumptionStrategy() = default;

    NESExecutionPlanPtr initializeExecutionPlan(InputQueryPtr inputQuery, NESTopologyPlanPtr nesTopologyPlan);

  private:

    void placeOperators(NESExecutionPlanPtr executionPlanPtr, NESTopologyGraphPtr nesTopologyGraphPtr,
                        OperatorPtr sourceOperator, vector<NESTopologyEntryPtr> sourceNodes);

    NESPlacementStrategyType getType() {
        return MinimumEnergyConsumption;
    }
};
}

#endif //NES_IMPL_OPTIMIZER_IMPL_MINIMUMENERGYCONSUMPTION_HPP_

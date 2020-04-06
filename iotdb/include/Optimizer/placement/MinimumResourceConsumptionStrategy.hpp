#ifndef NES_IMPL_OPTIMIZER_IMPL_MINIMUMRESOURCECONSUMPTION_HPP_
#define NES_IMPL_OPTIMIZER_IMPL_MINIMUMRESOURCECONSUMPTION_HPP_

#include <Optimizer/NESPlacementOptimizer.hpp>
namespace NES {

/**
 * @brief This class is responsible for placing the operators on common path among the sources such that overall
 * resource consumption will reduce.
 */
class MinimumResourceConsumptionStrategy : public NESPlacementOptimizer {

 public:
  MinimumResourceConsumptionStrategy() = default;
  ~MinimumResourceConsumptionStrategy() = default;

  NESExecutionPlanPtr initializeExecutionPlan(InputQueryPtr inputQuery, NESTopologyPlanPtr nesTopologyPlan);

 private:

  void placeOperators(NESExecutionPlanPtr executionPlanPtr, NESTopologyGraphPtr nesTopologyGraphPtr,
                      OperatorPtr sourceOperator, vector<NESTopologyEntryPtr> sourceNodes);

  NESPlacementStrategyType getType() {
    return MinimumResourceConsumption;
  }
};
}

#endif //NES_IMPL_OPTIMIZER_IMPL_MINIMUMRESOURCECONSUMPTION_HPP_

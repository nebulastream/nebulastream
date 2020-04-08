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
    ~MinimumEnergyConsumptionStrategy() = default;
    NESExecutionPlanPtr initializeExecutionPlan(InputQueryPtr inputQuery, NESTopologyPlanPtr nesTopologyPlan);

    static std::unique_ptr<MinimumEnergyConsumptionStrategy> create(){
        return std::make_unique<MinimumEnergyConsumptionStrategy>(MinimumEnergyConsumptionStrategy());
    }

  private:

    MinimumEnergyConsumptionStrategy() = default;

    void placeOperators(NESExecutionPlanPtr executionPlanPtr, NESTopologyGraphPtr nesTopologyGraphPtr,
                        OperatorPtr sourceOperator, vector<NESTopologyEntryPtr> sourceNodes);
    /**
     * @brief Finds all the nodes that can be used for performing FWD operator
     * @param sourceNodes
     * @param rootNode
     * @return
     */
    vector<NESTopologyEntryPtr> getCandidateNodesForFwdOperatorPlacement(const vector<NESTopologyEntryPtr>& sourceNodes,
                                                                         const NESTopologyEntryPtr rootNode) const;
};
}

#endif //NES_IMPL_OPTIMIZER_IMPL_MINIMUMENERGYCONSUMPTION_HPP_

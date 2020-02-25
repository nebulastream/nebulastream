#ifndef NES_IMPL_OPTIMIZER_IMPL_MINIMUMENERGYCONSUMPTION_HPP_
#define NES_IMPL_OPTIMIZER_IMPL_MINIMUMENERGYCONSUMPTION_HPP_

#include <Optimizer/NESPlacementOptimizer.hpp>
namespace NES {

/**
 * @brief This class is responsible for placing the operators on common path among the sources such that overall
 * resource consumption will reduce.
 */
class MinimumEnergyConsumption : public NESPlacementOptimizer {

  public:
    MinimumEnergyConsumption() = default;
    ~MinimumEnergyConsumption() = default;

    NESExecutionPlanPtr initializeExecutionPlan(InputQueryPtr inputQuery, NESTopologyPlanPtr nesTopologyPlan);

  private:

    void placeOperators(NESExecutionPlanPtr executionPlanPtr, NESTopologyGraphPtr nesTopologyGraphPtr,
                        OperatorPtr sourceOperator, vector<NESTopologyEntryPtr> sourceNodes);

    /**
     * @brief Add forward operators between source and sink nodes.
     * @param sourceNodes : list of source nodes
     * @param rootNode : sink node
     * @param nesExecutionPlanPtr : nes execution plan
     */
    void addForwardOperators(vector<NESTopologyEntryPtr> sourceNodes, NESTopologyEntryPtr rootNode,
                             NESExecutionPlanPtr nesExecutionPlanPtr);
};
}

#endif //NES_IMPL_OPTIMIZER_IMPL_MINIMUMENERGYCONSUMPTION_HPP_

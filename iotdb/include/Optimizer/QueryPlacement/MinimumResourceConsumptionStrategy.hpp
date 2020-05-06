#ifndef NES_IMPL_OPTIMIZER_IMPL_MINIMUMRESOURCECONSUMPTION_HPP_
#define NES_IMPL_OPTIMIZER_IMPL_MINIMUMRESOURCECONSUMPTION_HPP_

#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
namespace NES {

/**
 * @brief This class is responsible for placing the operators on common path among the sources such that overall
 * resource consumption will reduce.
 */
class MinimumResourceConsumptionStrategy : public BasePlacementStrategy {

  public:
    ~MinimumResourceConsumptionStrategy() = default;
    NESExecutionPlanPtr initializeExecutionPlan(InputQueryPtr inputQuery, NESTopologyPlanPtr nesTopologyPlan);

    static std::unique_ptr<MinimumResourceConsumptionStrategy> create() {
        return std::make_unique<MinimumResourceConsumptionStrategy>(MinimumResourceConsumptionStrategy());
    }

  private:
    MinimumResourceConsumptionStrategy() = default;

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
}// namespace NES

#endif//NES_IMPL_OPTIMIZER_IMPL_MINIMUMRESOURCECONSUMPTION_HPP_

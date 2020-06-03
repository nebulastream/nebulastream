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
    NESExecutionPlanPtr initializeExecutionPlan(QueryPlanPtr queryPlan, NESTopologyPlanPtr nesTopologyPlan, StreamCatalogPtr streamCatalog);

    static std::unique_ptr<MinimumResourceConsumptionStrategy> create(NESTopologyPlanPtr nesTopologyPlan) {
        return std::make_unique<MinimumResourceConsumptionStrategy>(MinimumResourceConsumptionStrategy(nesTopologyPlan));
    }

  private:
    MinimumResourceConsumptionStrategy(NESTopologyPlanPtr nesTopologyPlan);

    void placeOperators(NESExecutionPlanPtr executionPlanPtr, NESTopologyGraphPtr nesTopologyGraphPtr,
                        LogicalOperatorNodePtr sourceOperator, std::vector<NESTopologyEntryPtr> sourceNodes);

    /**
     * @brief Finds all the nodes that can be used for performing FWD operator
     * @param sourceNodes
     * @param rootNode
     * @return
     */
    std::vector<NESTopologyEntryPtr> getCandidateNodesForFwdOperatorPlacement(const std::vector<NESTopologyEntryPtr>& sourceNodes,
                                                                              const NESTopologyEntryPtr rootNode) const;
};
}// namespace NES

#endif//NES_IMPL_OPTIMIZER_IMPL_MINIMUMRESOURCECONSUMPTION_HPP_

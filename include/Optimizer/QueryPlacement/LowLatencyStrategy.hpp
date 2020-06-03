#ifndef NES_IMPL_OPTIMIZER_IMPL_LOWLINKLATENCY_HPP_
#define NES_IMPL_OPTIMIZER_IMPL_LOWLINKLATENCY_HPP_

#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>

namespace NES {

/**
 * @brief This class is used for operator placement on the path with low link latency between sources and destination.
 * Also, the operators are places such that the overall query latency will be as low as possible.
 *
 * Following are the rules used:
 *
 * Path Selection:
 * * Distinct paths with low link latency
 *
 * Operator Placement:
 * * Non-blocking operators closer to source
 * * Replicate operators when possible
 *
 */
class LowLatencyStrategy : public BasePlacementStrategy {

  public:
    ~LowLatencyStrategy(){};

    NESExecutionPlanPtr initializeExecutionPlan(QueryPlanPtr queryPlan, NESTopologyPlanPtr nesTopologyPlan, StreamCatalogPtr streamCatalogs);

    static std::unique_ptr<LowLatencyStrategy> create(NESTopologyPlanPtr nesTopologyPlan) {
        return std::make_unique<LowLatencyStrategy>(LowLatencyStrategy(nesTopologyPlan));
    }

  private:
    LowLatencyStrategy(NESTopologyPlanPtr nesTopologyPlan);

    void placeOperators(NESExecutionPlanPtr executionPlanPtr, NESTopologyGraphPtr nesTopologyGraphPtr,
                        LogicalOperatorNodePtr operatorPtr, std::vector<NESTopologyEntryPtr> sourceNodes);
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

#endif//NES_IMPL_OPTIMIZER_IMPL_LOWLINKLATENCY_HPP_

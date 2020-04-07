#ifndef NES_IMPL_OPTIMIZER_IMPL_LOWLINKLATENCY_HPP_
#define NES_IMPL_OPTIMIZER_IMPL_LOWLINKLATENCY_HPP_

#include <Optimizer/BasePlacementStrategy.hpp>

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
    LowLatencyStrategy() {};
    ~LowLatencyStrategy() {};

    NESExecutionPlanPtr initializeExecutionPlan(InputQueryPtr inputQuery, NESTopologyPlanPtr nesTopologyPlan);

  private:

    void placeOperators(NESExecutionPlanPtr executionPlanPtr, NESTopologyGraphPtr nesTopologyGraphPtr,
                        OperatorPtr operatorPtr, vector<NESTopologyEntryPtr> sourceNodes);
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

#endif //NES_IMPL_OPTIMIZER_IMPL_LOWLINKLATENCY_HPP_

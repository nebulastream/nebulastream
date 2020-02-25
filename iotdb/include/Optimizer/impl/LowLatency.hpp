#ifndef NES_IMPL_OPTIMIZER_IMPL_LOWLINKLATENCY_HPP_
#define NES_IMPL_OPTIMIZER_IMPL_LOWLINKLATENCY_HPP_

#include <Optimizer/NESPlacementOptimizer.hpp>

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
class LowLatency : public NESPlacementOptimizer {

  public:
    LowLatency() {};
    ~LowLatency() {};

    NESExecutionPlanPtr initializeExecutionPlan(InputQueryPtr inputQuery, NESTopologyPlanPtr nesTopologyPlan);

  private:

    void placeOperators(NESExecutionPlanPtr executionPlanPtr, NESTopologyGraphPtr nesTopologyGraphPtr,
                        OperatorPtr operatorPtr, vector<NESTopologyEntryPtr> sourceNodes);

    /**
     * @brief Add forward operators between source and sink nodes.
     * @param sourceNodes : list of source nodes
     * @param rootNode : sink node
     * @param nesExecutionPlanPtr : nes execution plan
     */
    void addForwardOperators(vector<NESTopologyEntryPtr> sourceNodes, NESTopologyEntryPtr rootNode,
                             NESExecutionPlanPtr nesExecutionPlanPtr) const;
};
}

#endif //NES_IMPL_OPTIMIZER_IMPL_LOWLINKLATENCY_HPP_

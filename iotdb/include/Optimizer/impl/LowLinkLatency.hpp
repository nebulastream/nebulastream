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
class LowLinkLatency : public NESPlacementOptimizer {

  public:
    LowLinkLatency() {};
    ~LowLinkLatency() {};

    NESExecutionPlanPtr initializeExecutionPlan(InputQueryPtr inputQuery, NESTopologyPlanPtr nesTopologyPlan);

    void placeOperators(NESExecutionPlanPtr executionPlanPtr,
                        const NESTopologyGraphPtr nesTopologyGraphPtr,
                        OperatorPtr operatorPtr,
                        deque<NESTopologyEntryPtr> sourceNodes);
};
}

#endif //NES_IMPL_OPTIMIZER_IMPL_LOWLINKLATENCY_HPP_

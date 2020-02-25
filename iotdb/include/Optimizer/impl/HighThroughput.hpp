#ifndef NES_IMPL_OPTIMIZER_IMPL_HIGHTHROUGHPUT_HPP_
#define NES_IMPL_OPTIMIZER_IMPL_HIGHTHROUGHPUT_HPP_

#include <Optimizer/NESPlacementOptimizer.hpp>

namespace NES {

/**
 * @brief This class is responsible for placing operators on high capacity links such that the overall query throughput
 * will increase.
 */
class HighThroughput: public NESPlacementOptimizer {

  public:
    HighThroughput() = default;
    ~HighThroughput() = default;

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

#endif //NES_IMPL_OPTIMIZER_IMPL_HIGHTHROUGHPUT_HPP_

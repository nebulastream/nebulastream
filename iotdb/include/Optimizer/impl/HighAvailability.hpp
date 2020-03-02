#ifndef NES_IMPL_OPTIMIZER_IMPL_HIGHAVAILABILITY_HPP_
#define NES_IMPL_OPTIMIZER_IMPL_HIGHAVAILABILITY_HPP_

#include <Optimizer/NESPlacementOptimizer.hpp>

namespace NES {

class Operator;
typedef std::shared_ptr<Operator> OperatorPtr;

class HighAvailability : public NESPlacementOptimizer {

  public:
    HighAvailability() = default;
    ~HighAvailability() = default;

    NESExecutionPlanPtr initializeExecutionPlan(InputQueryPtr inputQuery, NESTopologyPlanPtr nesTopologyPlan);

  private:

    /**
     * This method is responsible for placing the operators to the nes nodes and generating ExecutionNodes.
     * @param nesExecutionPlanPtr : graph containing the information about the execution nodes.
     * @param nesTopologyGraphPtr : nes Topology graph used for extracting information about the nes topology.
     * @param sourceNodePtr : sensor nodes which can act as source.
     *
     * @throws exception if the operator can't be placed anywhere.
     */
    void placeOperators(NESExecutionPlanPtr nesExecutionPlanPtr, NESTopologyGraphPtr nesTopologyGraphPtr,
                        OperatorPtr sourceOperator, vector<NESTopologyEntryPtr> sourceNodes);

    /**
     * @brief Add forward operators between source and sink nodes.
     * @param rootNode : sink node
     * @param nesExecutionPlanPtr : nes execution plan
     */
    void addForwardOperators(vector<NESTopologyEntryPtr> pathForPlacement,
                             NESExecutionPlanPtr nesExecutionPlanPtr) const;
};

}
#endif //NES_IMPL_OPTIMIZER_IMPL_HIGHAVAILABILITY_HPP_

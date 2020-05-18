#ifndef BOTTOMUP_HPP
#define BOTTOMUP_HPP

#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <iostream>

namespace NES {

using namespace std;

/**\brief:
 *          This class implements Bottom Up placement strategy. In this strategy, the source and sink operators are
 *          placed at respective nes nodes but rest of the operators are placed starting near to the source and then
 *          if the resources are not available they are placed on a node neighbouring to the node or one level up.
 */
class BottomUpStrategy : public BasePlacementStrategy {
  public:
    ~BottomUpStrategy(){};

    NESExecutionPlanPtr initializeExecutionPlan(QueryPtr inputQuery, NESTopologyPlanPtr nesTopologyPlan);

    static std::unique_ptr<BottomUpStrategy> create() {
        return std::make_unique<BottomUpStrategy>(BottomUpStrategy());
    }

  private:
    BottomUpStrategy() = default;

    /**
     * This method is responsible for placing the operators to the nes nodes and generating ExecutionNodes.
     * @param executionPlanPtr : graph containing the information about the execution nodes.
     * @param nesTopologyGraphPtr : nes Topology graph used for extracting information about the nes topology.
     * @param sourceOperator : sensor nodes which act as the source source.
     * @param sourceNodes : sensor nodes which act as the source source.
     *
     * @throws exception if the operator can't be placed anywhere.
     */
    void placeOperators(NESExecutionPlanPtr executionPlanPtr, NESTopologyGraphPtr nesTopologyGraphPtr,
                        LogicalOperatorNodePtr sourceOperator, vector<NESTopologyEntryPtr> sourceNodes);

    /**
     * @brief Finds all the nodes that can be used for performing FWD operator
     * @param sourceNodes
     * @param rootNode
     * @return
     */
    std::vector<NESTopologyEntryPtr> getCandidateNodesForFwdOperatorPlacement(const vector<NESTopologyEntryPtr>& sourceNodes,
                                                                              const NESTopologyEntryPtr rootNode) const;
};
}// namespace NES

#endif//BOTTOMUP_HPP

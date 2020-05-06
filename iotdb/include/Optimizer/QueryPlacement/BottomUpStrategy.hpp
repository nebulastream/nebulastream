#ifndef BOTTOMUP_HPP
#define BOTTOMUP_HPP

#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <iostream>

namespace NES {

class Operator;
typedef std::shared_ptr<Operator> OperatorPtr;

using namespace std;

/**\brief:
 *          This class implements Bottom Up placement strategy. In this strategy, the source and sink operators are
 *          placed at respective nes nodes but rest of the operators are placed starting near to the source and then
 *          if the resources are not available they are placed on a node neighbouring to the node or one level up.
 */
class BottomUpStrategy : public BasePlacementStrategy {
  public:
    ~BottomUpStrategy(){};

    NESExecutionPlanPtr initializeExecutionPlan(InputQueryPtr inputQuery, NESTopologyPlanPtr nesTopologyPlan);

    static std::unique_ptr<BottomUpStrategy> create() {
        return std::make_unique<BottomUpStrategy>(BottomUpStrategy());
    }

  private:
    BottomUpStrategy() = default;

    // This structure hold information about the current operator to place and previously placed parent operator.
    // It helps in deciding if the operator is to be placed in the nes node where the parent operator was placed
    // or on another suitable neighbouring node.
    struct ProcessOperator {
        ProcessOperator(OperatorPtr operatorPtr, ExecutionNodePtr executionNodePtr) {
            this->operatorToProcess = operatorPtr;
            this->parentExecutionNode = executionNodePtr;
        }

        OperatorPtr operatorToProcess;
        ExecutionNodePtr parentExecutionNode;
    };

    /**
     * This method is responsible for placing the operators to the nes nodes and generating ExecutionNodes.
     * @param executionPlanPtr : graph containing the information about the execution nodes.
     * @param nesTopologyGraphPtr : nes Topology graph used for extracting information about the nes topology.
     * @param sourceNodePtr : sensor nodes which can act as source.
     *
     * @throws exception if the operator can't be placed anywhere.
     */
    void placeOperators(NESExecutionPlanPtr executionPlanPtr, NESTopologyGraphPtr nesTopologyGraphPtr,
                        OperatorPtr sourceOperator, vector<NESTopologyEntryPtr> sourceNodes);

    /**
     * @brief Finds a suitable for node for the operator to be placed.
     * @param operatorToProcess
     * @param nesTopologyGraphPtr
     * @param sourceNodePtr
     * @return
     */
    NESTopologyEntryPtr findSuitableNESNodeForOperatorPlacement(const ProcessOperator& operatorToProcess,
                                                                NESTopologyGraphPtr nesTopologyGraphPtr,
                                                                NESTopologyEntryPtr sourceNodePtr);

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

#endif//BOTTOMUP_HPP

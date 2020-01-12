#ifndef IOTDB_BOTTOMUP_HPP
#define IOTDB_BOTTOMUP_HPP

#include <Operators/Operator.hpp>
#include <iostream>
#include "../NESPlacementOptimizer.hpp"

namespace NES {

using namespace std;

/**\brief:
 *          This class implements Bottom Up placement strategy. In this strategy, the source and sink operators are
 *          placed at respective nes nodes but rest of the operators are placed starting near to the source and then
 *          if the resources are not available they are placed on a node neighbouring to the node or one level up.
 */
class BottomUp : public NESPlacementOptimizer {
 public:
  BottomUp() {};
  ~BottomUp() {};

  NESExecutionPlan initializeExecutionPlan(InputQueryPtr inputQuery, NESTopologyPlanPtr nesTopologyPlan);

 private:

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
   * @param executionGraph : graph containing the information about the execution nodes.
   * @param nesTopologyPlan : nes Topology plan used for extracting information about the nes topology.
   * @param sourceOperators : List of source operators.
   * @param sourceNodes : List of sensor nodes which can act as source.
   *
   * @throws exception if the operator can't be placed anywhere.
   */
  void placeOperators(NESExecutionPlan executionGraph, NESTopologyPlanPtr nesTopologyPlan,
                      vector<OperatorPtr> sourceOperators, deque<NESTopologyEntryPtr> sourceNodes);

  // finds a suitable for node for the operator to be placed.
  NESTopologyEntryPtr findSuitableNESNodeForOperatorPlacement(const ProcessOperator &operatorToProcess,
                                                              NESTopologyPlanPtr &nesTopologyPlan,
                                                              deque<NESTopologyEntryPtr> &sourceNodes);

  // This method returns all the source operators in the user input query
  vector<OperatorPtr> getSourceOperators(OperatorPtr root);

  // This method returns all sensor nodes that act as the source in the nes topology.
  deque<NESTopologyEntryPtr> getSourceNodes(NESTopologyPlanPtr nesTopologyPlan,
                                            std::string streamName);

};
}

#endif //IOTDB_BOTTOMUP_HPP

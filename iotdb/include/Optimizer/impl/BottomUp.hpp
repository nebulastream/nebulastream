#ifndef IOTDB_BOTTOMUP_HPP
#define IOTDB_BOTTOMUP_HPP

#include <Optimizer/FogPlacementOptimizer.hpp>
#include <Operators/Operator.hpp>
#include <iostream>

namespace iotdb {

using namespace std;

/**\brief:
 *          This class implements Bottom Up placement strategy. In this strategy, the source and sink operators are
 *          placed at respective fog nodes but rest of the operators are placed starting near to the source and then
 *          if the resources are not available they are placed on a node neighbouring to the node or one level up.
 */
class BottomUp : public FogPlacementOptimizer {
 private:

 public:
  BottomUp() {};

  FogExecutionPlan initializeExecutionPlan(InputQuery inputQuery, FogTopologyPlanPtr fogTopologyPlan);

 private:

  // This structure hold information about the current operator to place and previously placed parent operator.
  // It helps in deciding if the operator is to be placed in the fog node where the parent operator was placed
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
   * This method is responsible for placing the operators to the fog nodes and generating ExecutionNodes.
   * @param executionGraph : graph containing the information about the execution nodes.
   * @param fogTopologyPlan : Fog Topology plan used for extracting information about the fog topology.
   * @param sourceOperators : List of source operators.
   * @param sourceNodes : List of sensor nodes which can act as source.
   *
   * @throws exception if the operator can't be placed anywhere.
   */
  void placeOperators(FogExecutionPlan executionGraph, FogTopologyPlanPtr fogTopologyPlan,
                      vector<OperatorPtr> sourceOperators, deque<FogTopologyEntryPtr> sourceNodes);

  // finds a suitable for node for the operator to be placed.
  FogTopologyEntryPtr findSuitableFogNodeForOperatorPlacement(const ProcessOperator &operatorToProcess,
                                                              FogTopologyPlanPtr &fogTopologyPlan,
                                                              deque<FogTopologyEntryPtr> &sourceNodes);

  // This method returns all the source operators in the user input query
  vector<OperatorPtr> getSourceOperators(OperatorPtr root);

  // This method returns all sensor nodes that act as the source in the fog topology.
  deque<FogTopologyEntryPtr> getSourceNodes(FogTopologyPlanPtr fogTopologyPlan,
                                                   std::string streamName);

};
}

#endif //IOTDB_BOTTOMUP_HPP

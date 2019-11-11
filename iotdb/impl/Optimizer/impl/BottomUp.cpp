#include "Optimizer/impl/BottomUp.hpp"
#include <Operators/Operator.hpp>
#include <iostream>
#include <exception>

using namespace iotdb;
using namespace std;

FogExecutionPlan BottomUp::initializeExecutionPlan(InputQuery inputQuery, FogTopologyPlanPtr fogTopologyPlan) {

  FogExecutionPlan executionGraph;
  const OperatorPtr &sinkOperator = inputQuery.getRoot();
  const string &streamName = inputQuery.source_stream.getName();
  const vector<OperatorPtr> &sourceOperators = getSourceOperators(sinkOperator);
  const deque<FogTopologyEntryPtr> &sourceNodes = getSourceNodes(fogTopologyPlan, streamName);

  if (sourceNodes.empty()) {
    std::cout << "Unable to find the target source";
    throw "No source found in the topology";
  }

  placeOperators(executionGraph, fogTopologyPlan, sourceOperators, sourceNodes);

  removeNonResidentOperators(executionGraph);

  completeExecutionGraphWithFogTopology(executionGraph, fogTopologyPlan);

  //FIXME: We are assuming that throughout the pipeline the schema would not change.
  Schema &schema = inputQuery.source_stream.getSchema();
  addSystemGeneratedSourceSinkOperators(schema, executionGraph);

  return executionGraph;
}

void BottomUp::placeOperators(FogExecutionPlan executionGraph, FogTopologyPlanPtr fogTopologyPlan,
                              vector<OperatorPtr> sourceOperators, deque<FogTopologyEntryPtr> sourceNodes) {

  FogTopologyEntryPtr &targetSource = sourceNodes[0];

  //lambda to convert source optr vector to a friendly struct
  deque<ProcessOperator> operatorsToProcess;
  transform(sourceOperators.begin(), sourceOperators.end(), back_inserter(operatorsToProcess),
            [](OperatorPtr optr) {
              return ProcessOperator(optr, nullptr);
            });

  while (!operatorsToProcess.empty()) {

    ProcessOperator operatorToProcess = operatorsToProcess.front();
    operatorsToProcess.pop_front();

    OperatorPtr &optr = operatorToProcess.operatorToProcess;
    if (optr->isScheduled()) {
      continue;
    }

    // find the node where the operator will be executed
    FogTopologyEntryPtr node = findSuitableFogNodeForOperatorPlacement(operatorToProcess, fogTopologyPlan,
                                                                       sourceNodes);

    if ((node == nullptr) or node->getRemainingCpuCapacity() <= 0) {
      // throw and exception that scheduling can't be done
      throw "Can't schedule The Query";
    }

    // Reduce the processing capacity by 1
    // FIXME: Bring some logic here where the cpu capacity is reduced based on operator workload
    node->reduceCpuCapacity(1);

    // If the selected Fog node was already used by another operator for placement then do not create a
    // new execution node rather add operator to existing node.
    if (executionGraph.hasVertex(node->getId())) {

      const ExecutionNodePtr &existingExecutionNode = executionGraph.getExecutionNode(node->getId());

      string oldOperatorName = existingExecutionNode->getOperatorName();
      string newName =
          oldOperatorName + "=>" + operatorTypeToString[optr->getOperatorType()] + "(OP-"
              + std::to_string(optr->operatorId) + ")";

      existingExecutionNode->setOperatorName(newName);
      existingExecutionNode->addChildOperatorId(optr->operatorId);

      optr->markScheduled(true);
      if (optr->parent != nullptr) {
        operatorsToProcess.emplace_back(ProcessOperator(optr->parent, existingExecutionNode));
      }
    } else {

      // Create a new execution node
      const ExecutionNodePtr &newExecutionNode =
          executionGraph.createExecutionNode(operatorTypeToString[optr->getOperatorType()] + "(OP-"
                                                 + std::to_string(optr->operatorId) + ")",
                                             to_string(
                                                 node->getId()),
                                             node, optr);

      optr->markScheduled(true);
      if (optr->parent != nullptr) {
        operatorsToProcess.emplace_back(ProcessOperator(optr->parent, newExecutionNode));
      }
    }
  }

  const FogGraphPtr &fogGraphPtr = fogTopologyPlan->getFogGraph();
  deque<FogTopologyEntryPtr> candidateNodes = getCandidateFogNodes(fogGraphPtr, targetSource);
  while (!candidateNodes.empty()) {
    shared_ptr<FogTopologyEntry> node = candidateNodes.front();
    candidateNodes.pop_front();
    if (node->getCpuCapacity() == node->getRemainingCpuCapacity()) {
      executionGraph.createExecutionNode("FWD", to_string(node->getId()), node, nullptr);
      node->reduceCpuCapacity(1);
    }
  }

}

FogTopologyEntryPtr BottomUp::findSuitableFogNodeForOperatorPlacement(const ProcessOperator &operatorToProcess,
                                                            FogTopologyPlanPtr &fogTopologyPlan,
                                                            deque<FogTopologyEntryPtr> &sourceNodes) {

  FogTopologyEntryPtr node;

  if (operatorToProcess.operatorToProcess->getOperatorType() == OperatorType::SINK_OP) {
    node = fogTopologyPlan->getRootNode();
  } else if (operatorToProcess.operatorToProcess->getOperatorType() == OperatorType::SOURCE_OP) {
    node = sourceNodes.front();
    sourceNodes.pop_front();
  } else {
    FogTopologyEntryPtr &fogNode = operatorToProcess.parentExecutionNode->getFogNode();

    //if the previous parent node still have capacity. Use it for further operator assignment
    if (fogNode->getRemainingCpuCapacity() > 0) {
      node = fogNode;
    } else {
      // else find the neighbouring higher level nodes connected to it
      const vector<FogTopologyLinkPtr> &allEdgesToNode = fogTopologyPlan->getFogGraph()->getAllEdgesFromNode(
          fogNode);

      vector<FogTopologyEntryPtr> neighbouringNodes;

      transform(allEdgesToNode.begin(), allEdgesToNode.end(), back_inserter(neighbouringNodes),
                [](FogTopologyLinkPtr fogLink) {
                  return fogLink->getDestNode();
                });

      FogTopologyEntryPtr neighbouringNodeWithMaxCPU = nullptr;

      for (FogTopologyEntryPtr neighbouringNode: neighbouringNodes) {

        if ((neighbouringNodeWithMaxCPU == nullptr) ||
            (neighbouringNode->getRemainingCpuCapacity() >
                neighbouringNodeWithMaxCPU->getRemainingCpuCapacity())) {

          neighbouringNodeWithMaxCPU = neighbouringNode;
        }
      }

      if ((neighbouringNodeWithMaxCPU == nullptr) or
          neighbouringNodeWithMaxCPU->getRemainingCpuCapacity() <= 0) {
        node = nullptr;
      } else if (neighbouringNodeWithMaxCPU->getRemainingCpuCapacity() > 0) {
        node = neighbouringNodeWithMaxCPU;
      }
    }
  }

  return node;
};

// This method returns all the source operators in the user input query
vector<OperatorPtr> BottomUp::getSourceOperators(OperatorPtr root) {

  vector<OperatorPtr> listOfSourceOperators;
  deque<OperatorPtr> bfsTraverse;
  bfsTraverse.push_back(root);

  while (!bfsTraverse.empty()) {

    while (!bfsTraverse.empty()) {
      auto &optr = bfsTraverse.front();
      bfsTraverse.pop_front();

      if (optr->getOperatorType() == OperatorType::SOURCE_OP) {
        listOfSourceOperators.push_back(optr);
      }

      vector<OperatorPtr> &children = optr->childs;
      copy(children.begin(), children.end(), back_inserter(bfsTraverse));
    }
  }
  return listOfSourceOperators;
};

// This method returns all sensor nodes that act as the source in the fog topology.
deque<FogTopologyEntryPtr> BottomUp::getSourceNodes(FogTopologyPlanPtr fogTopologyPlan,
                                                 std::string streamName) {

  const FogTopologyEntryPtr &rootNode = fogTopologyPlan->getRootNode();
  deque<FogTopologyEntryPtr> listOfSourceNodes;
  deque<FogTopologyEntryPtr> bfsTraverse;
  bfsTraverse.push_back(rootNode);

  while (!bfsTraverse.empty()) {
    auto &node = bfsTraverse.front();
    bfsTraverse.pop_front();

    if (node->getEntryType() == FogNodeType::Sensor) {

      FogTopologySensorNodePtr ptr = std::static_pointer_cast<FogTopologySensorNode>(node);

      if (ptr->getSensorType() == streamName) {
        listOfSourceNodes.push_back(node);
      }
    }

    const vector<FogTopologyLinkPtr> &edgesToNode = fogTopologyPlan->getFogGraph()->getAllEdgesToNode(node);

    for (FogTopologyLinkPtr edgeToNode: edgesToNode) {
      bfsTraverse.push_back(edgeToNode->getSourceNode());
    }
  }

  return listOfSourceNodes;
}

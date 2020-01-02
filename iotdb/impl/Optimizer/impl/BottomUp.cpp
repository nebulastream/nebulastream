#include "Optimizer/impl/BottomUp.hpp"
#include <Operators/Operator.hpp>
#include <iostream>

using namespace iotdb;
using namespace std;

NESExecutionPlan BottomUp::initializeExecutionPlan(
    InputQueryPtr inputQuery, NESTopologyPlanPtr nesTopologyPlan) {

  NESExecutionPlan executionGraph;
  const OperatorPtr& sinkOperator = inputQuery->getRoot();
  const string& streamName = inputQuery->source_stream->getName();
  const vector<OperatorPtr>& sourceOperators = getSourceOperators(sinkOperator);
  IOTDB_DEBUG("BottomUp: try to place the following source operators:")
  for (OperatorPtr op : sourceOperators)
    IOTDB_DEBUG("\t BottomUp: " << op->toString())

  //TODO: this is the old version without catalog
//    const deque<NESTopologyEntryPtr>& sourceNodes = getSourceNodes(nesTopologyPlan, streamName);
  const deque<NESTopologyEntryPtr>& sourceNodes = StreamCatalog::instance()
      .getSourceNodesForLogicalStream(streamName);

  if (sourceNodes.empty()) {
    IOTDB_ERROR("BottomUp: Unable to find the target source: " << streamName);
    throw Exception("No source found in the topology for stream " + streamName);
  }

  IOTDB_DEBUG("BottomUp: try to place on following source nodes:")
  for (NESTopologyEntryPtr node : sourceNodes)
    IOTDB_DEBUG("\t BottomUp: " << node->toString())

  placeOperators(executionGraph, nesTopologyPlan, sourceOperators, sourceNodes);

  removeNonResidentOperators(executionGraph);

  completeExecutionGraphWithNESTopology(executionGraph, nesTopologyPlan);

  //FIXME: We are assuming that throughout the pipeline the schema would not change.
  Schema& schema = inputQuery->source_stream->getSchema();
  addSystemGeneratedSourceSinkOperators(schema, executionGraph);

  return executionGraph;
}

void BottomUp::placeOperators(NESExecutionPlan executionGraph,
                              NESTopologyPlanPtr nesTopologyPlan,
                              vector<OperatorPtr> sourceOperators,
                              deque<NESTopologyEntryPtr> sourceNodes) {

  NESTopologyEntryPtr& targetSource = sourceNodes[0];

  //lambda to convert source optr vector to a friendly struct
  deque<ProcessOperator> operatorsToProcess;
  transform(sourceOperators.begin(), sourceOperators.end(),
            back_inserter(operatorsToProcess), [](OperatorPtr optr) {
              return ProcessOperator(optr, nullptr);
            });

  while (!operatorsToProcess.empty()) {

    ProcessOperator operatorToProcess = operatorsToProcess.front();
    operatorsToProcess.pop_front();

    OperatorPtr& optr = operatorToProcess.operatorToProcess;
    if (optr->isScheduled()) {
      continue;
    }

    IOTDB_DEBUG("BottomUp: try to place operator " << optr->toString())

    // find the node where the operator will be executed
    NESTopologyEntryPtr node = findSuitableNESNodeForOperatorPlacement(
        operatorToProcess, nesTopologyPlan, sourceNodes);

    if (node == nullptr) {
      // throw and exception that scheduling can't be done
      IOTDB_ERROR("BottomUp: Can not schedule the operator. No node found.");
      throw std::runtime_error("Can not schedule the operator. No node found.");
    } else if (node->getRemainingCpuCapacity() <= 0) {
      IOTDB_ERROR(
          "BottomUp: Can not schedule the operator. No free resource available capacity is=" << node->getRemainingCpuCapacity());
      throw std::runtime_error(
          "Can not schedule the operator. No free resource available.");
    }

    IOTDB_DEBUG("BottomUp: suitable placement for operator " << optr->toString() << " is " << node->toString())

    // Reduce the processing capacity by 1
    // FIXME: Bring some logic here where the cpu capacity is reduced based on operator workload
    node->reduceCpuCapacity(1);

    // If the selected nes node was already used by another operator for placement then do not create a
    // new execution node rather add operator to existing node.
    if (executionGraph.hasVertex(node->getId())) {
      IOTDB_DEBUG("BottomUp: node " << node->toString() << " was already used by other deployment")

      const ExecutionNodePtr& existingExecutionNode = executionGraph
          .getExecutionNode(node->getId());

      string oldOperatorName = existingExecutionNode->getOperatorName();
      string newName = oldOperatorName + "=>"
          + operatorTypeToString[optr->getOperatorType()] + "(OP-"
          + std::to_string(optr->operatorId) + ")";

      existingExecutionNode->setOperatorName(newName);
      existingExecutionNode->addChildOperatorId(optr->operatorId);

      optr->markScheduled(true);
      if (optr->parent != nullptr) {
        operatorsToProcess.emplace_back(
            ProcessOperator(optr->parent, existingExecutionNode));
      }
    } else {
      IOTDB_DEBUG("BottomUp: create new execution node " << node->toString())

      // Create a new execution node
      const ExecutionNodePtr& newExecutionNode = executionGraph
          .createExecutionNode(
          operatorTypeToString[optr->getOperatorType()] + "(OP-"
              + std::to_string(optr->operatorId) + ")",
          to_string(node->getId()), node, optr);

      optr->markScheduled(true);
      if (optr->parent != nullptr) {
        operatorsToProcess.emplace_back(
            ProcessOperator(optr->parent, newExecutionNode));
      }
    }
  }

  //TODO: what is happening here?
  const NESGraphPtr& nesGraphPtr = nesTopologyPlan->getNESGraph();
  deque<NESTopologyEntryPtr> candidateNodes = getCandidateNESNodes(
      nesGraphPtr, targetSource);
  while (!candidateNodes.empty()) {
    shared_ptr<NESTopologyEntry> node = candidateNodes.front();
    candidateNodes.pop_front();
    if (node->getCpuCapacity() == node->getRemainingCpuCapacity()) {
      executionGraph.createExecutionNode("FWD", to_string(node->getId()), node,
                                         nullptr);
      node->reduceCpuCapacity(1);
    }
  }

}

NESTopologyEntryPtr BottomUp::findSuitableNESNodeForOperatorPlacement(
    const ProcessOperator& operatorToProcess,
    NESTopologyPlanPtr& nesTopologyPlan,
    deque<NESTopologyEntryPtr>& sourceNodes) {

  NESTopologyEntryPtr node;

  if (operatorToProcess.operatorToProcess->getOperatorType()
      == OperatorType::SINK_OP) {
    node = nesTopologyPlan->getRootNode();
    return node;
    IOTDB_DEBUG("BottomUp: place sink on root node " << node->toString())
  } else if (operatorToProcess.operatorToProcess->getOperatorType()
      == OperatorType::SOURCE_OP) {
    node = sourceNodes.front();
    sourceNodes.pop_front();
    IOTDB_DEBUG("BottomUp: place source on source node " << node->toString())
    return node;
  } else {
    NESTopologyEntryPtr& nesNode = operatorToProcess.parentExecutionNode
        ->getNESNode();
    //if the previous parent node still have capacity. Use it for further operator assignment
    if (nesNode->getRemainingCpuCapacity() > 0) {
      node = nesNode;
      IOTDB_DEBUG("BottomUp: place operator on node with remaining capacity " << node->toString())
    } else {
      IOTDB_DEBUG("BottomUp: place operator on node neighbouring node")

      // else find the neighbouring higher level nodes connected to it
      const vector<NESTopologyLinkPtr>& allEdgesToNode = nesTopologyPlan
          ->getNESGraph()->getAllEdgesFromNode(nesNode);

      vector<NESTopologyEntryPtr> neighbouringNodes;

      transform(allEdgesToNode.begin(), allEdgesToNode.end(),
                back_inserter(neighbouringNodes),
                [](NESTopologyLinkPtr nesLink) {
                  return nesLink->getDestNode();
                });

      NESTopologyEntryPtr neighbouringNodeWithMaxCPU = nullptr;

      for (NESTopologyEntryPtr neighbouringNode : neighbouringNodes) {

        if ((neighbouringNodeWithMaxCPU == nullptr)
            || (neighbouringNode->getRemainingCpuCapacity()
                > neighbouringNodeWithMaxCPU->getRemainingCpuCapacity())) {

          neighbouringNodeWithMaxCPU = neighbouringNode;
        }
      }

      if ((neighbouringNodeWithMaxCPU == nullptr)
          or neighbouringNodeWithMaxCPU->getRemainingCpuCapacity() <= 0) {
        node = nullptr;
      } else if (neighbouringNodeWithMaxCPU->getRemainingCpuCapacity() > 0) {
        node = neighbouringNodeWithMaxCPU;
      }
    }
  }

  return node;
}
;

// This method returns all the source operators in the user input query
vector<OperatorPtr> BottomUp::getSourceOperators(OperatorPtr root) {

  vector<OperatorPtr> listOfSourceOperators;
  deque<OperatorPtr> bfsTraverse;
  bfsTraverse.push_back(root);

  while (!bfsTraverse.empty()) {

    while (!bfsTraverse.empty()) {
      auto& optr = bfsTraverse.front();
      bfsTraverse.pop_front();

      if (optr->getOperatorType() == OperatorType::SOURCE_OP) {
        listOfSourceOperators.push_back(optr);
      }

      vector<OperatorPtr>& children = optr->childs;
      copy(children.begin(), children.end(), back_inserter(bfsTraverse));
    }
  }
  return listOfSourceOperators;
}
;

// This method returns all sensor nodes that act as the source in the nes topology.
deque<NESTopologyEntryPtr> BottomUp::getSourceNodes(
    NESTopologyPlanPtr nesTopologyPlan, std::string streamName) {

//  assert(0);
//TODO:should not be called anymore in the future
  const NESTopologyEntryPtr& rootNode = nesTopologyPlan->getRootNode();
  deque<NESTopologyEntryPtr> listOfSourceNodes;
  deque<NESTopologyEntryPtr> bfsTraverse;
  bfsTraverse.push_back(rootNode);

  while (!bfsTraverse.empty()) {
    auto& node = bfsTraverse.front();
    bfsTraverse.pop_front();

    if (node->getEntryType() == NESNodeType::Sensor) {

      NESTopologySensorNodePtr ptr = std::static_pointer_cast<
          NESTopologySensorNode>(node);

      if (ptr->getPhysicalStreamName() == streamName) {
        listOfSourceNodes.push_back(node);
      }
    }

    const vector<NESTopologyLinkPtr>& edgesToNode =
        nesTopologyPlan->getNESGraph()->getAllEdgesToNode(node);

    for (NESTopologyLinkPtr edgeToNode : edgesToNode) {
      bfsTraverse.push_back(edgeToNode->getSourceNode());
    }
  }

  return listOfSourceNodes;
}

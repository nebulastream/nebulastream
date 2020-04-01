#include <Optimizer/BasePlacementStrategy.hpp>
#include <iostream>
#include <exception>
#include <Optimizer/placement/BottomUpStrategy.hpp>
#include <Optimizer/placement/TopDownStrategy.hpp>
#include <Optimizer/placement/LowLatencyStrategy.hpp>
#include <Operators/Operator.hpp>
#include <SourceSink/SourceCreator.hpp>
#include <SourceSink/SinkCreator.hpp>
#include <Optimizer/utils/PathFinder.hpp>
#include <Optimizer/placement/HighAvailabilityStrategy.hpp>
#include <Optimizer/placement/HighThroughputStrategy.hpp>
#include <Optimizer/placement/MinimumResourceConsumptionStrategy.hpp>
#include <Optimizer/placement/MinimumEnergyConsumptionStrategy.hpp>
#include <Util/Logger.hpp>
#include <SourceSink/SenseSource.hpp>

namespace NES {

std::unique_ptr<BasePlacementStrategy> BasePlacementStrategy::getStrategy(std::string placementStrategyName) {

  if (stringToPlacementStrategyType.find(placementStrategyName) == stringToPlacementStrategyType.end()) {
    throw std::invalid_argument("BasePlacementStrategy: Unknown placement strategy name " + placementStrategyName);
  }

  switch (stringToPlacementStrategyType[placementStrategyName]) {
    case TopDown:
        return TopDownStrategy::create();
    case BottomUp:
        return BottomUpStrategy::create();
    case LowLatency:
        return LowLatencyStrategy::create();
    case HighThroughput:
        return HighThroughputStrategy::create();
    case MinimumResourceConsumption:
      return MinimumResourceConsumptionStrategy::create();
    case MinimumEnergyConsumption:
      return MinimumEnergyConsumptionStrategy::create();
    case HighAvailability:
        return HighAvailabilityStrategy::create();
  }
}

static const int zmqDefaultPort = 5555;
//FIXME: Currently the system is not designed for multiple children. Therefore, the logic is ignoring the fact
// that there could be more than one child. Once the code generator able to deal with it this logic need to be
// fixed.
void BasePlacementStrategy::addSystemGeneratedSourceSinkOperators(
    SchemaPtr schema , NESExecutionPlanPtr nesExecutionPlanPtr) {

  const std::shared_ptr<ExecutionGraph>& exeGraph = nesExecutionPlanPtr
      ->getExecutionGraph();
  const vector<ExecutionVertex>& executionNodes = exeGraph->getAllVertex();
  for (ExecutionVertex executionNode : executionNodes) {
    ExecutionNodePtr executionNodePtr = executionNode.ptr;

    //Convert fwd operator
    if (executionNodePtr->getOperatorName() == "FWD") {
      convertFwdOptr(schema, executionNodePtr);
      continue;
    }

    OperatorPtr rootOperator = executionNodePtr->getRootOperator();
    if (rootOperator == nullptr) {
      continue;
    }

    if (rootOperator->getOperatorType() != SOURCE_OP) {
      //create sys introduced src operator
      //Note: the source zmq is always located at localhost
      OperatorPtr sysSrcOptr = createSourceOperator(
          createZmqSource(schema, "localhost", zmqDefaultPort));

      //bind sys introduced operators to each other
      rootOperator->setChildren({sysSrcOptr});
      sysSrcOptr->setParent(rootOperator);

      //Update the operator name
      string optrName = executionNodePtr->getOperatorName();
      optrName = "SOURCE(SYS)=>" + optrName;
      executionNodePtr->setOperatorName(optrName);

      //change the root operator to point to the sys introduced sink operator
      executionNodePtr->setRootOperator(sysSrcOptr);
    }

    OperatorPtr traverse = rootOperator;
    while (traverse->getParent() != nullptr) {
      traverse = traverse->getParent();
    }

    if (traverse->getOperatorType() != SINK_OP) {

      //create sys introduced sink operator

      const vector<ExecutionEdge>& edges = exeGraph->getAllEdgesFromNode(
          executionNodePtr);
      //FIXME: More than two sources are not supported feature at this moment. Once the feature is available please
      // fix the source code
      const string& destHostName = edges[0].ptr->getDestination()->getNESNode()
          ->getIp();

      const OperatorPtr sysSinkOptr = createSinkOperator(
          createZmqSink(schema, destHostName, zmqDefaultPort));

      //Update the operator name
      string optrName = executionNodePtr->getOperatorName();
      optrName = optrName + "=>SINK(SYS)";
      executionNodePtr->setOperatorName(optrName);

      //bind sys introduced operators to each other
      sysSinkOptr->setChildren({traverse});
      traverse->setParent(sysSinkOptr);
    }


  }
}

void BasePlacementStrategy::convertFwdOptr(
    SchemaPtr schema , ExecutionNodePtr executionNodePtr) const {

  //create sys introduced src and sink operators
  //Note: src operator is using localhost because src zmq will run locally
  const OperatorPtr sysSrcOptr = createSourceOperator(
      createZmqSource(schema, "localhost", zmqDefaultPort));
  const OperatorPtr sysSinkOptr = createSinkOperator(
      createZmqSink(schema, "localhost", zmqDefaultPort));

  sysSrcOptr->setParent(sysSinkOptr);
  sysSinkOptr->setChildren({sysSrcOptr});

  executionNodePtr->setRootOperator(sysSrcOptr);
  executionNodePtr->setOperatorName("SOURCE(SYS)=>SINK(SYS)");
}

static const char* const NO_OPERATOR = "NO-OPERATOR";

void BasePlacementStrategy::fillExecutionGraphWithTopologyInformation(NESExecutionPlanPtr nesExecutionPlanPtr,
                                                                      NESTopologyPlanPtr nesTopologyPtr) {

  NES_DEBUG("BasePlacementStrategy: Filling the execution graph with topology information.")
  const vector<NESTopologyLinkPtr>& allEdges = nesTopologyPtr->getNESTopologyGraph()->getAllEdges();
  NES_DEBUG("BasePlacementStrategy: Get all edges in the Topology and iterate over all edges to identify the nodes"
            " that are not part of the execution graph.")

  for (NESTopologyLinkPtr nesLink : allEdges) {

    size_t srcId = nesLink->getSourceNode()->getId();
    size_t destId = nesLink->getDestNode()->getId();
    size_t linkId = nesLink->getId();
    size_t linkCapacity = nesLink->getLinkCapacity();
    size_t linkLatency = nesLink->getLinkLatency();

    ExecutionNodePtr srcExecutionNode, destExecutionNode;

    NES_DEBUG("BasePlacementStrategy: If sourceNode present in the execution graph then use it, else create "
              "an empty execution node with no operator.")
    if (nesExecutionPlanPtr->hasVertex(srcId)) {
      srcExecutionNode = nesExecutionPlanPtr->getExecutionNode(srcId);
    } else {
      srcExecutionNode = nesExecutionPlanPtr->createExecutionNode(NO_OPERATOR, to_string(srcId),
                                                                  nesLink->getSourceNode(), nullptr);
    }

    NES_DEBUG("BasePlacementStrategy: If destinationNode present in the execution graph then use it, else create "
              "an empty execution node with no operator.")
    if (nesExecutionPlanPtr->hasVertex(destId)) {
      destExecutionNode = nesExecutionPlanPtr->getExecutionNode(destId);
    } else {
      destExecutionNode = nesExecutionPlanPtr->createExecutionNode(NO_OPERATOR, to_string(destId),
                                                                   nesLink->getDestNode(), nullptr);
    }

    NES_DEBUG("BasePlacementStrategy: create the execution node link.")
    nesExecutionPlanPtr->createExecutionNodeLink(linkId, srcExecutionNode, destExecutionNode, linkCapacity,
                                                 linkLatency);
  }
}

void BasePlacementStrategy::setUDFSFromSampleOperatorToSenseSources(InputQueryPtr inputQuery) {

  //TODO: this is only the first try, it should be replaced by the new functions offered by the nbew log query plan
  const OperatorPtr sinkOperator = inputQuery->getRoot();

  // FIXME: current implementation assumes that we have only one source stream and therefore only one source operator.
  const string& streamName = inputQuery->getSourceStream()->getName();
  const OperatorPtr sourceOperatorPtr = getSourceOperator(sinkOperator);

  string udfs = inputQuery->getUdsf();
  if (udfs != "") {
    NES_DEBUG(
        "BasePlacementStrategy::setUDFSFromSampleOperatorToSenseSources: a sample operator is provided")
    if (dynamic_cast<SenseSource*>(sourceOperatorPtr.get())) {
      SenseSource* node = dynamic_cast<SenseSource*>(sourceOperatorPtr.get());
      node->setUdsf(udfs);
    } else {
      NES_ERROR("BasePlacementStrategy: cast to sample node failed")
      throw new Exception("BasePlacementStrategy cast to sample op failed");
    }
  }
}

OperatorPtr BasePlacementStrategy::getSourceOperator(OperatorPtr root) {

  deque<OperatorPtr> operatorTraversQueue = {root};

  while (!operatorTraversQueue.empty()) {
    auto optr = operatorTraversQueue.front();
    operatorTraversQueue.pop_front();

    if (optr->getOperatorType() == OperatorType::SOURCE_OP) {
      return optr;
    }

    vector<OperatorPtr> children = optr->getChildren();
    copy(children.begin(), children.end(), back_inserter(operatorTraversQueue));
  }

  return nullptr;
};

void BasePlacementStrategy::addForwardOperators(vector<NESTopologyEntryPtr> candidateNodes,
                                                NESExecutionPlanPtr nesExecutionPlanPtr) {

  //FIXME: This may not work when we have two different queries deployed on the same path.
  NES_DEBUG(
      "BasePlacementStrategy: Add FWD operator on the node where no operator from the query has been placed");
  for (NESTopologyEntryPtr candidateNode: candidateNodes) {
    if (candidateNode->getCpuCapacity() == candidateNode->getRemainingCpuCapacity()) {
      nesExecutionPlanPtr->createExecutionNode("FWD", to_string(candidateNode->getId()), candidateNode,
          /**executableOperator**/nullptr);
      candidateNode->reduceCpuCapacity(1);
    }
  }
}

}

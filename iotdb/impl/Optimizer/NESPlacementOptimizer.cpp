#include <Optimizer/NESPlacementOptimizer.hpp>
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

std::shared_ptr<NESPlacementOptimizer> NESPlacementOptimizer::getOptimizer(std::string placementStrategyName) {

  if (stringToPlacementStrategyType.find(placementStrategyName) == stringToPlacementStrategyType.end()) {
    throw std::invalid_argument("NESPlacementOptimizer: Unknown placement strategy name " + placementStrategyName);
  }

  switch (stringToPlacementStrategyType[placementStrategyName]) {
    case TopDown:return std::make_unique<TopDownStrategy>(TopDownStrategy());
    case BottomUp:return std::make_unique<BottomUpStrategy>(BottomUpStrategy());
    case LowLatency:return std::make_unique<LowLatencyStrategy>(LowLatencyStrategy());
    case HighThroughput:return std::make_unique<HighThroughputStrategy>(HighThroughputStrategy());
    case MinimumResourceConsumption:
      return std::make_unique<MinimumResourceConsumptionStrategy>(MinimumResourceConsumptionStrategy());
    case MinimumEnergyConsumption:
      return std::make_unique<MinimumEnergyConsumptionStrategy>(MinimumEnergyConsumptionStrategy());
    case HighAvailability:return std::make_unique<HighAvailabilityStrategy>(HighAvailabilityStrategy());
  }
}

static const int zmqDefaultPort = 5555;
//FIXME: Currently the system is not designed for multiple children. Therefore, the logic is ignoring the fact
// that there could be more than one child. Once the code generator able to deal with it this logic need to be
// fixed.
void NESPlacementOptimizer::addSystemGeneratedSourceSinkOperators(
    const Schema& schema, NESExecutionPlanPtr nesExecutionPlanPtr) {

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

void NESPlacementOptimizer::convertFwdOptr(
    const Schema& schema, ExecutionNodePtr executionNodePtr) const {

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

void NESPlacementOptimizer::completeExecutionGraphWithNESTopology(NESExecutionPlanPtr nesExecutionPlanPtr,
                                                                  NESTopologyPlanPtr nesTopologyPtr) {

  const vector<NESTopologyLinkPtr>& allEdges = nesTopologyPtr->getNESTopologyGraph()->getAllEdges();

  for (NESTopologyLinkPtr nesLink : allEdges) {

    size_t srcId = nesLink->getSourceNode()->getId();
    size_t destId = nesLink->getDestNode()->getId();
    size_t linkId = nesLink->getId();
    size_t linkCapacity = nesLink->getLinkCapacity();
    size_t linkLatency = nesLink->getLinkLatency();

    if (nesExecutionPlanPtr->hasVertex(srcId)) {
      const ExecutionNodePtr srcExecutionNode = nesExecutionPlanPtr->getExecutionNode(srcId);
      if (nesExecutionPlanPtr->hasVertex(destId)) {
        const ExecutionNodePtr destExecutionNode = nesExecutionPlanPtr->getExecutionNode(destId);
        nesExecutionPlanPtr->createExecutionNodeLink(linkId, srcExecutionNode, destExecutionNode, linkCapacity,
                                                     linkLatency);
      } else {
        const ExecutionNodePtr destExecutionNode = nesExecutionPlanPtr
            ->createExecutionNode("NO-OPERATOR", to_string(destId),
                                  nesLink->getDestNode(), nullptr);
        nesExecutionPlanPtr->createExecutionNodeLink(linkId, srcExecutionNode, destExecutionNode, linkCapacity,
                                                     linkLatency);
      }
    } else {

      const ExecutionNodePtr srcExecutionNode = nesExecutionPlanPtr->createExecutionNode("NO-OPERATOR",
                                                                                         to_string(srcId),
                                                                                         nesLink->getSourceNode(),
                                                                                         nullptr);
      if (nesExecutionPlanPtr->hasVertex(destId)) {
        const ExecutionNodePtr destExecutionNode = nesExecutionPlanPtr->getExecutionNode(destId);
        nesExecutionPlanPtr->createExecutionNodeLink(linkId, srcExecutionNode, destExecutionNode, linkCapacity,
                                                     linkLatency);
      } else {
        const ExecutionNodePtr destExecutionNode = nesExecutionPlanPtr->createExecutionNode("NO-OPERATOR",
                                                                                            to_string(destId),
                                                                                            nesLink->getDestNode(),
                                                                                            nullptr);
        nesExecutionPlanPtr->createExecutionNodeLink(linkId, srcExecutionNode, destExecutionNode, linkCapacity,
                                                     linkLatency);
      }
    }
  }
};
void NESPlacementOptimizer::setUDFSFromSampleOperatorToSenseSources(InputQueryPtr inputQuery) {

  //TODO: this is only the first try, it should be replaced by the new functions offered by the nbew log query plan
  const OperatorPtr sinkOperator = inputQuery->getRoot();

  // FIXME: current implementation assumes that we have only one source stream and therefore only one source operator.
  const string& streamName = inputQuery->getSourceStream()->getName();
  const OperatorPtr sourceOperatorPtr = getSourceOperator(sinkOperator);

  string udfs = inputQuery->getUdsf();
  if (udfs != "") {
    NES_DEBUG(
        "NESPlacementOptimizer::setUDFSFromSampleOperatorToSenseSources: a sample operator is provided")
    if (dynamic_cast<SenseSource*>(sourceOperatorPtr.get())) {
      SenseSource* node = dynamic_cast<SenseSource*>(sourceOperatorPtr.get());
      node->setUdsf(udfs);
    } else {
      NES_ERROR("NESPlacementOptimizer: cast to sample node failed")
      throw new Exception("NESPlacementOptimizer cast to sample op failed");
    }
  }
}

OperatorPtr NESPlacementOptimizer::getSourceOperator(OperatorPtr root) {

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

void NESPlacementOptimizer::addForwardOperators(NESPlacementStrategyType nesPlacementStrategyType,
                                                vector<NESTopologyEntryPtr> sourceNodes,
                                                NESTopologyEntryPtr rootNode,
                                                NESExecutionPlanPtr nesExecutionPlanPtr) {

  PathFinder pathFinder;
  std::vector<NESTopologyEntryPtr> candidateNodes;
  NES_DEBUG("NESPlacementOptimizer: Find the path used for performing the placement based on the strategy type");

  switch (nesPlacementStrategyType) {
    case TopDown:
    case BottomUp: {
      for (NESTopologyEntryPtr targetSource: sourceNodes) {
        //Find the list of nodes connecting the source and destination nodes
        std::vector<NESTopologyEntryPtr> nodesOnPath = pathFinder.findPathBetween(targetSource, rootNode);
        candidateNodes.insert(candidateNodes.end(), nodesOnPath.begin(), nodesOnPath.end());
      }
      break;
    }
    case HighThroughput: {
      for (NESTopologyEntryPtr targetSource: sourceNodes) {
        //Find the list of nodes connecting the source and destination nodes
        std::vector<NESTopologyEntryPtr> nodesOnPath = pathFinder.findPathWithMaxBandwidth(targetSource, rootNode);
        candidateNodes.insert(candidateNodes.end(), nodesOnPath.begin(), nodesOnPath.end());
      }
      break;
    }
    case LowLatency: {
      for (NESTopologyEntryPtr targetSource: sourceNodes) {
        //Find the list of nodes connecting the source and destination nodes
        std::vector<NESTopologyEntryPtr> nodesOnPath = pathFinder.findPathWithMinLinkLatency(targetSource, rootNode);
        candidateNodes.insert(candidateNodes.end(), nodesOnPath.begin(), nodesOnPath.end());
      }
      break;
    }
    case MinimumEnergyConsumption:
    case MinimumResourceConsumption: {
      map<NESTopologyEntryPtr, std::vector<NESTopologyEntryPtr>>
          pathMap = pathFinder.findUniquePathBetween(sourceNodes, rootNode);

      for (auto[key, value] : pathMap) {
        candidateNodes.insert(candidateNodes.end(), value.begin(), value.end());
      }
      break;
    }
  }

  //FIXME: This may not work when we have two different queries deployed on the same path.
  NES_DEBUG(
      "NESPlacementOptimizer: Add FWD operator on the node where no operator from the query has been placed");
  for (NESTopologyEntryPtr candidateNode: candidateNodes) {
    if (candidateNode->getCpuCapacity() == candidateNode->getRemainingCpuCapacity()) {
      nesExecutionPlanPtr->createExecutionNode("FWD", to_string(candidateNode->getId()), candidateNode,
          /**executableOperator**/nullptr);
      candidateNode->reduceCpuCapacity(1);
    }
  }
};

}

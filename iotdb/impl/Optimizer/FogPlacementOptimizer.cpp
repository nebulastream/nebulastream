#include <Optimizer/FogPlacementOptimizer.hpp>
#include <iostream>
#include <Optimizer/impl/BottomUp.hpp>
#include <Optimizer/impl/TopDown.hpp>
#include <Runtime/DataSink.hpp>

namespace iotdb {

FogPlacementOptimizer *FogPlacementOptimizer::getOptimizer(std::string optimizerName) {
  if (optimizerName == "BottomUp") {
    return new BottomUp();
  } else if (optimizerName == "TopDown") {
    return new TopDown();
  } else {
    throw "Unknown optimizer type : " + optimizerName;
  }
}

void FogPlacementOptimizer::removeNonResidentOperators(FogExecutionPlan graph) {

  const vector<ExecutionVertex> &executionNodes = graph.getExecutionGraph().getAllVertex();
  for (ExecutionVertex executionNode: executionNodes) {
    OperatorPtr &rootOperator = executionNode.ptr->getRootOperator();
    vector<int> &childOperatorIds = executionNode.ptr->getChildOperatorIds();
    invalidateUnscheduledOperators(rootOperator, childOperatorIds);
  }
}

void FogPlacementOptimizer::invalidateUnscheduledOperators(OperatorPtr &rootOperator, vector<int> &childOperatorIds) {
  vector<OperatorPtr> &childs = rootOperator->childs;
  OperatorPtr &parent = rootOperator->parent;

  if (rootOperator == nullptr) {
    return;
  }

  if (parent != nullptr) {
    if (std::find(childOperatorIds.begin(), childOperatorIds.end(), parent->operatorId) != childOperatorIds.end()) {
      invalidateUnscheduledOperators(parent, childOperatorIds);
    } else {
      rootOperator->parent = nullptr;
    }
  }

  for (size_t i = 0; i < childs.size(); i++) {
    OperatorPtr child = childs[i];
    if (std::find(childOperatorIds.begin(), childOperatorIds.end(), child->operatorId) != childOperatorIds.end()) {
      invalidateUnscheduledOperators(child, childOperatorIds);
    } else {
      childs.erase(childs.begin() + i);
    }
  }
}

static const int zmqDefaultPort = 5555;
//FIXME: Currently the system is not designed for multiple children. Therefore, the logic is ignoring the fact
// that there could be more than one child. Once the code generator able to deal with it this logic need to be
// fixed.
void FogPlacementOptimizer::addSystemGeneratedSourceSinkOperators(const Schema &schema, FogExecutionPlan graph) {

  const ExecutionGraph &exeGraph = graph.getExecutionGraph();
  const vector<ExecutionVertex> &executionNodes = exeGraph.getAllVertex();
  for (ExecutionVertex executionNode: executionNodes) {
    ExecutionNodePtr &executionNodePtr = executionNode.ptr;

    //Convert fwd operator
    if (executionNodePtr->getOperatorName() == "FWD") {
      convertFwdOptr(schema, executionNodePtr);
      continue;
    }

    OperatorPtr &rootOperator = executionNodePtr->getRootOperator();
    if (rootOperator == nullptr) {
      continue;
    }

    if (rootOperator->getOperatorType() != SOURCE_OP) {
      //create sys introduced src operator
      //Note: the source zmq is always located at localhost
      const OperatorPtr &sysSrcOptr = createSourceOperator(createZmqSource(schema, "localhost", zmqDefaultPort));

      //bind sys introduced operators to each other
      rootOperator->childs = {sysSrcOptr};
      sysSrcOptr->parent = rootOperator;

      //Update the operator name
      string optrName = executionNodePtr->getOperatorName();
      optrName = "SOURCE(SYS)=>" + optrName;
      executionNodePtr->setOperatorName(optrName);

      //change the root operator to point to the sys introduced sink operator
      executionNodePtr->setRootOperator(sysSrcOptr);
    }

    OperatorPtr traverse = rootOperator;
    while (traverse->parent != nullptr) {
      traverse = traverse->parent;
    }

    if (traverse->getOperatorType() != SINK_OP) {

      //create sys introduced sink operator

      const vector<ExecutionEdge> &edges = exeGraph.getAllEdgesFromNode(executionNodePtr);
      //FIXME: More than two sources are not supported feature at this moment. Once the feature is available please
      // fix the source code
      const string &destHostName = edges[0].ptr->getDestination()->getFogNode()->getHostname();
      const OperatorPtr &sysSinkOptr = createSinkOperator(createZmqSink(schema, destHostName, zmqDefaultPort));

      //Update the operator name
      string optrName = executionNodePtr->getOperatorName();
      optrName = optrName + "=>SINK(SYS)";
      executionNodePtr->setOperatorName(optrName);

      //bind sys introduced operators to each other
      sysSinkOptr->childs = {traverse};
      traverse->parent = sysSinkOptr;
    }

  }
}

void FogPlacementOptimizer::convertFwdOptr(const Schema &schema,
                                           ExecutionNodePtr &executionNodePtr) const {//create sys introduced src and sink operators

  //Note: src operator is using localhost because src zmq will run locally
  const OperatorPtr &sysSrcOptr = createSourceOperator(createZmqSource(schema, "localhost", zmqDefaultPort));
  const OperatorPtr &sysSinkOptr = createSinkOperator(createZmqSink(schema, "localhost", zmqDefaultPort));

  sysSrcOptr->parent = sysSinkOptr;
  sysSinkOptr->childs = {sysSrcOptr};

  executionNodePtr->setRootOperator(sysSrcOptr);
  executionNodePtr->setOperatorName("SOURCE(SYS)=>SINK(SYS)");
}

void FogPlacementOptimizer::completeExecutionGraphWithFogTopology(FogExecutionPlan graph,
                                                                  FogTopologyPlanPtr sharedPtr) {

  const vector<FogTopologyLinkPtr> &allEdges = sharedPtr->getFogGraph().getAllEdges();

  for (FogTopologyLinkPtr fogLink: allEdges) {

    size_t srcId = fogLink->getSourceNode()->getId();
    size_t destId = fogLink->getDestNode()->getId();
    if (graph.hasVertex(srcId)) {
      const ExecutionNodePtr &srcExecutionNode = graph.getExecutionNode(srcId);
      if (graph.hasVertex(destId)) {
        const ExecutionNodePtr &destExecutionNode = graph.getExecutionNode(destId);
        graph.createExecutionNodeLink(srcExecutionNode, destExecutionNode);
      } else {
        const ExecutionNodePtr &destExecutionNode = graph.createExecutionNode("empty",
                                                                              to_string(destId),
                                                                              fogLink->getDestNode(),
                                                                              nullptr);
        graph.createExecutionNodeLink(srcExecutionNode, destExecutionNode);
      }
    } else {

      const ExecutionNodePtr &srcExecutionNode = graph.createExecutionNode("empty", to_string(srcId),
                                                                           fogLink->getSourceNode(),
                                                                           nullptr);
      if (graph.hasVertex(destId)) {
        const ExecutionNodePtr &destExecutionNode = graph.getExecutionNode(destId);
        graph.createExecutionNodeLink(srcExecutionNode, destExecutionNode);
      } else {
        const ExecutionNodePtr &destExecutionNode = graph.createExecutionNode("empty",
                                                                              to_string(destId),
                                                                              fogLink->getDestNode(),
                                                                              nullptr);
        graph.createExecutionNodeLink(srcExecutionNode, destExecutionNode);
      }
    }
  }
};

deque<FogTopologyEntryPtr> FogPlacementOptimizer::getCandidateFogNodes(const FogGraph &fogGraph,
                                                                       const FogTopologyEntryPtr &targetSource) const {

  deque<FogTopologyEntryPtr> candidateNodes = {};

  const FogTopologyEntryPtr &rootNode = fogGraph.getRoot();

  deque<int> visitedNodes = {};
  candidateNodes.push_back(rootNode);

  while (!candidateNodes.empty()) {

    FogTopologyEntryPtr &back = candidateNodes.back();

    if (back->getId() == targetSource->getId()) {
      break;
    }

    const vector<FogTopologyLinkPtr> &allEdgesToNode = fogGraph.getAllEdgesToNode(back);

    if (!allEdgesToNode.empty()) {
      bool found = false;
      for (FogTopologyLinkPtr fogLink: allEdgesToNode) {
        const FogTopologyEntryPtr &sourceNode = fogLink->getSourceNode();
        if (!count(visitedNodes.begin(), visitedNodes.end(), sourceNode->getId())) {
          candidateNodes.push_back(sourceNode);
          found = true;
          break;
        }
      }
      if (!found) {
        candidateNodes.pop_back();
      }
    } else {
      candidateNodes.pop_back();
    }

    if (!count(visitedNodes.begin(), visitedNodes.end(), back->getId())) {
      visitedNodes.push_front(back->getId());
    }

  }
  return candidateNodes;
};

}

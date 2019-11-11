#include <Optimizer/FogPlacementOptimizer.hpp>
#include <iostream>
#include <Optimizer/impl/BottomUp.hpp>
#include <Optimizer/impl/TopDown.hpp>

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


  //TODO: Check if we have to make the child object null;
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

/**\brief:
 *         This is the interface for base optimizer that needed to be implemented by any new query optimizer.
 *
 */

#ifndef IOTDB_FOGPLACEMENTOPTIMIZER_HPP
#define IOTDB_FOGPLACEMENTOPTIMIZER_HPP

#include <Optimizer/FogExecutionPlan.hpp>
#include <iostream>
#include <Topology/FogTopologyPlan.hpp>
#include <Topology/FogTopologyManager.hpp>

namespace iotdb {
class FogPlacementOptimizer {
 private:

 public:
  FogPlacementOptimizer() {};

  /**
   * Returns an execution graph based on the input query and fog topology.
   * @param inputQuery
   * @param fogTopologyPlan
   * @return
   */
  virtual FogExecutionPlan initializeExecutionPlan(InputQuery inputQuery, FogTopologyPlanPtr fogTopologyPlan) = 0;

  void completeExecutionGraphWithFogTopology(FogExecutionPlan graph, FogTopologyPlanPtr sharedPtr) {

    const vector<FogEdge> &allEdges = sharedPtr->getFogGraph().getAllEdges();

    for (FogEdge fogEdge: allEdges) {

      FogTopologyLinkPtr &topologyLink = fogEdge.ptr;
      size_t srcId = topologyLink->getSourceNode()->getId();
      size_t destId = topologyLink->getDestNode()->getId();
      if (graph.hasVertex(srcId)) {
        const ExecutionNodePtr &srcExecutionNode = graph.getExecutionNode(srcId);
        if (graph.hasVertex(destId)) {
          const ExecutionNodePtr &destExecutionNode = graph.getExecutionNode(destId);
          graph.createExecutionNodeLink(srcExecutionNode, destExecutionNode);
        } else {
          const ExecutionNodePtr &destExecutionNode = graph.createExecutionNode("empty",
                                                                                to_string(destId),
                                                                                topologyLink->getDestNode(),
                                                                                nullptr);
          graph.createExecutionNodeLink(srcExecutionNode, destExecutionNode);
        }
      } else {

        const ExecutionNodePtr &srcExecutionNode = graph.createExecutionNode("empty", to_string(srcId),
                                                                             topologyLink->getSourceNode(),
                                                                             nullptr);
        if (graph.hasVertex(destId)) {
          const ExecutionNodePtr &destExecutionNode = graph.getExecutionNode(destId);
          graph.createExecutionNodeLink(srcExecutionNode, destExecutionNode);
        } else {
          const ExecutionNodePtr &destExecutionNode = graph.createExecutionNode("empty",
                                                                                to_string(destId),
                                                                                topologyLink->getDestNode(),
                                                                                nullptr);
          graph.createExecutionNodeLink(srcExecutionNode, destExecutionNode);
        }
      }
    }
  };

  /**
   * Factory method returning different kind of optimizer.
   * @param optimizerName
   * @return instance of type BaseOptimizer
   */
  static FogPlacementOptimizer *getOptimizer(std::string optimizerName);

  /**
* Get all candidate node from sink to the target source node.
* @param fogGraph
* @param targetSource
* @return deque containing Fog nodes with top element being sink node and bottom most being the targetSource node.
*/
  deque<FogTopologyEntryPtr> getCandidateFogNodes(const FogGraph &fogGraph,
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

      const vector<FogEdge> &allEdgesToNode = fogGraph.getAllEdgesToNode(back);

      if (!allEdgesToNode.empty()) {
        bool found = false;
        for (FogEdge edge: allEdgesToNode) {
          const FogTopologyEntryPtr &sourceNode = edge.ptr->getSourceNode();
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
};

}

#endif //IOTDB_FOGPLACEMENTOPTIMIZER_HPP

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

    };

}

#endif //IOTDB_FOGPLACEMENTOPTIMIZER_HPP

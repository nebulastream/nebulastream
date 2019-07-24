#ifndef IOTDB_BOTTOMUP_HPP
#define IOTDB_BOTTOMUP_HPP

#include <Optimizer/BaseOptimizer.hpp>
#include <Operators/Operator.hpp>
#include <iostream>

namespace iotdb {

    using namespace std;

    /**\brief:
     *          This class implements Highest Level First placement strategy. It is similar to Bottom Up strategy but
     *          here the operator at highest level (bottom most level) is placed at compatible node at highest level of
     *          the fog topology.
     */
    class BottomUp : public BaseOptimizer {
    public:
        BottomUp() {};

        OptimizedExecutionGraph prepareExecutionPlan(InputQuery inputQuery, FogTopologyPlanPtr fogTopologyPlan);

    private:

        struct ProcessOperator {
            ProcessOperator(OperatorPtr operatorPtr, ExecutionNodePtr executionNodePtr) {
                this->operatorToProcess = operatorPtr;
                this->childExecutionNode = executionNodePtr;
            }

            OperatorPtr operatorToProcess;
            ExecutionNodePtr childExecutionNode;
        };

        void
        placeOperators(OptimizedExecutionGraph executionGraph, FogTopologyPlanPtr fogTopologyPlan,
                       vector<OperatorPtr> sourceOptrs,
                       deque<FogTopologyEntryPtr> sourceNodes) {

            deque<ProcessOperator> operatorsToProcess;

            //lambda to convert source optr vector to a friendly struct
            transform(sourceOptrs.begin(), sourceOptrs.end(), back_inserter(operatorsToProcess), [](OperatorPtr optr) {
                return ProcessOperator(optr, nullptr);
            });

            while (!operatorsToProcess.empty()) {

                ProcessOperator operatorToProcess = operatorsToProcess.front();
                operatorsToProcess.pop_front();

                OperatorPtr &optr = operatorToProcess.operatorToProcess;
                if (optr->isScheduled()) {
                    continue;
                }

                // find the node where the operator will be scheduled

                FogTopologyEntryPtr node;

                //If the operator is the sink node
                if (operatorToProcess.operatorToProcess->parent == nullptr) {

                    node = fogTopologyPlan->getRootNode();
                }
                    //If the operator is not the source node
                else if (operatorToProcess.childExecutionNode != nullptr) {
                    FogTopologyEntryPtr &fogNode = operatorToProcess.childExecutionNode->getFogNode();

                    //if the previous child node still have capacity. Use it for further operator assignment
                    if (fogNode->getRemainingCpuCapacity() > 0) {
                        node = fogNode;
                    } else {
                        // else find the neighbouring higher level nodes connected to it
                        const vector<FogEdge> &allEdgesToNode = fogTopologyPlan->getFogGraph().getAllEdgesFromNode(
                                fogNode);

                        vector<FogTopologyEntryPtr> neighbouringNodes;

                        transform(allEdgesToNode.begin(), allEdgesToNode.end(), back_inserter(neighbouringNodes),
                                  [](FogEdge edge) {
                                      return edge.ptr->getDestNode();
                                  });

                        FogTopologyEntryPtr neighbouringNodeWithMaxCPU = nullptr;

                        for (FogTopologyEntryPtr neighbouringNode: neighbouringNodes) {

                            if ((neighbouringNodeWithMaxCPU == nullptr) ||
                                (neighbouringNode->getRemainingCpuCapacity() > neighbouringNodeWithMaxCPU->getRemainingCpuCapacity())) {

                                neighbouringNodeWithMaxCPU = neighbouringNode;
                            }
                        }

                        if ((neighbouringNodeWithMaxCPU == nullptr) or neighbouringNodeWithMaxCPU->getRemainingCpuCapacity() <= 0) {
                            //throw and exception that scheduling can't be done
                            throw "Can't schedule The Query";
                        }

                        if (neighbouringNodeWithMaxCPU->getRemainingCpuCapacity() > 0) {

                            node = neighbouringNodeWithMaxCPU;
                        }
                    }
                } else {
                    node = sourceNodes.front();
                    sourceNodes.pop_front();
                }

                if ((node == nullptr) or node->getRemainingCpuCapacity() <= 0) {
                    //throw and exception that scheduling can't be done
                    throw "Can't schedule The Query";
                }

                //Reduce the processing capacity
                node->reduceCpuCapacity(1);

                if (executionGraph.hasVertex(node->getId())) {

                    const ExecutionNodePtr &existingExecutionNode = executionGraph.getExecutionNode(node->getId());

                    string oldOperatorName = existingExecutionNode->getOperatorName();
                    string newName = optr->toString() + "\n : \n" + oldOperatorName;

                    existingExecutionNode->setOperatorName(newName);
                    existingExecutionNode->addExecutableOperator(optr);

                    optr->markScheduled(true);
                    if (optr->parent != nullptr) {
                        operatorsToProcess.emplace_back(ProcessOperator(optr->parent, existingExecutionNode));
                    }
                } else {

                    // Now create the executable node and the links to child nodes
                    const ExecutionNodePtr &newExecutionNode = executionGraph.createExecutionNode(optr->toString(),
                                                                                                  to_string(
                                                                                                          node->getId()),
                                                                                                  node, optr);

                    optr->markScheduled(true);
                    if (optr->parent != nullptr) {
                        operatorsToProcess.emplace_back(ProcessOperator(optr->parent, newExecutionNode));
                    }
                }
            }
        };

        static vector<OperatorPtr> getSourceOperators(OperatorPtr root) {

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

        static deque<FogTopologyEntryPtr> getSourceNodes(FogTopologyPlanPtr fogTopologyPlan) {

            const FogTopologyEntryPtr &rootNode = fogTopologyPlan->getRootNode();
            deque<FogTopologyEntryPtr> listOfSourceNodes;
            deque<FogTopologyEntryPtr> bfsTraverse;
            bfsTraverse.push_back(rootNode);


            while (!bfsTraverse.empty()) {
                auto &node = bfsTraverse.front();
                bfsTraverse.pop_front();

                if (node->getEntryType() == FogNodeType::Sensor) {
                    listOfSourceNodes.push_back(node);
                }


                const vector<FogEdge> &children = fogTopologyPlan->getFogGraph().getAllEdgesToNode(node);

                for (FogEdge child: children) {
                    bfsTraverse.push_back(child.ptr->getSourceNode());
                }
            }

            return listOfSourceNodes;

        };

        void completeExecutionGraphWithFogTopology(OptimizedExecutionGraph graph, FogTopologyPlanPtr sharedPtr) {

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
    };
}

#endif //IOTDB_BOTTOMUP_HPP

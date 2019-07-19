#ifndef IOTDB_HLF_HPP
#define IOTDB_HLF_HPP

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
    class HLF : public BaseOptimizer {
    public:
        ExecutionGraph prepareExecutionPlan(InputQuery inputQuery, FogTopologyPlan fogTopologyPlan);

    private:

        struct ProcessOperator {
            ProcessOperator(OperatorPtr operatorPtr, ExecutionNodePtr executionNodePtr) {
                this->operatorToProcess = operatorPtr;
                this->childExecutionNode = executionNodePtr;
            }

            OperatorPtr operatorToProcess;
            ExecutionNodePtr childExecutionNode;
        };

        static void
        placeOperators(OptimizedExecutionGraph executionGraph, FogTopologyPlan fogTopologyPlan,
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

                if (operatorToProcess.childExecutionNode != nullptr) {
                    FogTopologyEntryPtr &fogNode = operatorToProcess.childExecutionNode->getFogNode();

                    //if the previous child node still have capacity. Use it for further operator assignment
                    if (fogNode->getCpuCapacity() != 0) {
                        node = fogNode;
                    } else {
                        // else find the neighbouring higher level nodes connected to it
                        const vector<Edge> &allEdgesToNode = fogTopologyPlan.getFogGraph().getAllEdgesToNode(fogNode);

                        vector<FogTopologyEntryPtr> neighbouringNodes;

                        transform(allEdgesToNode.begin(), allEdgesToNode.end(), back_inserter(neighbouringNodes),
                                  [](Edge edge) {
                                      return edge.ptr->getSourceNode();
                                  });

                        FogTopologyEntryPtr neighbouringNodeWithMaxCPU = nullptr;

                        for (FogTopologyEntryPtr neighbouringNode: neighbouringNodes) {

                            if ((neighbouringNode == nullptr) ||
                                (neighbouringNode->getCpuCapacity() > neighbouringNodeWithMaxCPU->getCpuCapacity())) {

                                neighbouringNodeWithMaxCPU = neighbouringNode;
                            }
                        }

                        if (neighbouringNodeWithMaxCPU->getCpuCapacity() > 0) {

                            node = neighbouringNodeWithMaxCPU;
                        }
                    }
                } else {
                    node = sourceNodes.front();
                    sourceNodes.pop_front();
                }

                if (node == nullptr) {
                    //throw and exception that scheduling can't be done
                }

                // Now create the executable node and the links to child nodes
                const ExecutionNodePtr &executionNode = executionGraph.createExecutionNode(optr->toString(),
                                                                                           to_string(
                                                                                                   node->getId()),
                                                                                           node, optr);
                //Reduce the processing capacity
                node->reduceCpuCapacity(1);
                ExecutionNodePtr &childExecutionNode = operatorToProcess.childExecutionNode;

                if (childExecutionNode != nullptr) {
                    executionGraph.createExecutionNodeLink(executionNode, childExecutionNode);
                }
                optr->markScheduled(true);
                operatorsToProcess.emplace_back(ProcessOperator(optr->parent, executionNode));
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

        static deque<FogTopologyEntryPtr> getSourceNodes(FogTopologyPlan fogTopologyPlan) {

            const FogTopologyEntryPtr &rootNode = fogTopologyPlan.getRootNode();
            deque<FogTopologyEntryPtr> listOfSourceNodes;
            deque<FogTopologyEntryPtr> bfsTraverse;
            bfsTraverse.push_back(rootNode);


            while (!bfsTraverse.empty()) {
                auto &node = bfsTraverse.front();
                bfsTraverse.pop_front();

                if (node->getEntryType() == FogNodeType::Sensor) {
                    listOfSourceNodes.push_back(node);
                }


                const vector<Edge> &children = fogTopologyPlan.getFogGraph().getAllEdgesToNode(node);

                copy(children.begin(), children.end(), back_inserter(bfsTraverse));
            }

            return listOfSourceNodes;

        };
    };
}

#endif //IOTDB_HLF_HPP

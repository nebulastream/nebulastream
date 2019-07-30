#ifndef IOTDB_BOTTOMUP_HPP
#define IOTDB_BOTTOMUP_HPP

#include <Optimizer/FogPlacementOptimizer.hpp>
#include <Operators/Operator.hpp>
#include <iostream>

namespace iotdb {

    using namespace std;

    /**\brief:
     *          This class implements Bottom Up placement strategy. In this strategy, the source and sink operators are
     *          placed at respective fog nodes but rest of the operators are placed starting near to the source and then
     *          if the resources are not available they are placed on a node neighbouring to the node or one level up.
     */
    class BottomUp : public FogPlacementOptimizer {
    private:

    public:
        BottomUp() {};

        FogExecutionPlan initializeExecutionPlan(InputQuery inputQuery, FogTopologyPlanPtr fogTopologyPlan);

    private:

        // This structure hold information about the current operator to place and previously placed parent operator.
        // It helps in deciding if the operator is to be placed in the fog node where the parent operator was placed
        // or on another suitable neighbouring node.
        struct ProcessOperator {
            ProcessOperator(OperatorPtr operatorPtr, ExecutionNodePtr executionNodePtr) {
                this->operatorToProcess = operatorPtr;
                this->parentExecutionNode = executionNodePtr;
            }

            OperatorPtr operatorToProcess;
            ExecutionNodePtr parentExecutionNode;
        };

        /**
         * This method is responsible for placing the operators to the fog nodes and generating ExecutionNodes.
         * @param executionGraph : graph containing the information about the execution nodes.
         * @param fogTopologyPlan : Fog Topology plan used for extracting information about the fog topology.
         * @param sourceOperators : List of source operators.
         * @param sourceNodes : List of sensor nodes which can act as source.
         *
         * @throws exception if the operator can't be placed anywhere.
         */
        void placeOperators(FogExecutionPlan executionGraph, FogTopologyPlanPtr fogTopologyPlan,
                            vector<OperatorPtr> sourceOperators, deque<FogTopologyEntryPtr> sourceNodes) {

            deque<ProcessOperator> operatorsToProcess;

            //lambda to convert source optr vector to a friendly struct
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
                    string newName = oldOperatorName + "=>" + optr->toString();

                    existingExecutionNode->setOperatorName(newName);
                    existingExecutionNode->addExecutableOperator(optr);

                    optr->markScheduled(true);
                    if (optr->parent != nullptr) {
                        operatorsToProcess.emplace_back(ProcessOperator(optr->parent, existingExecutionNode));
                    }
                } else {

                    // Create a new execution node
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

        // finds a suitable for node for the operator to be placed.
        FogTopologyEntryPtr findSuitableFogNodeForOperatorPlacement(const ProcessOperator &operatorToProcess,
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

        // This method returns all sensor nodes that can act as the source in the fog topology.
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
    };
}

#endif //IOTDB_BOTTOMUP_HPP

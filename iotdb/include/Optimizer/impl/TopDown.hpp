#ifndef IOTDB_TOPDOWN_HPP
#define IOTDB_TOPDOWN_HPP

#include <Optimizer/FogPlacementOptimizer.hpp>
#include <Operators/Operator.hpp>
#include <stack>

namespace iotdb {

    using namespace std;

    class TopDown : public FogPlacementOptimizer {

    public:
        FogExecutionPlan prepareExecutionPlan(InputQuery inputQuery, FogTopologyPlanPtr fogTopologyPlan);

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

        void placeOperators(FogExecutionPlan executionPlan, InputQuery query, FogTopologyPlanPtr fogTopologyPlanPtr) {

            const OperatorPtr &sinkOperator = query.getRoot();
            const FogGraph &fogGraph = fogTopologyPlanPtr->getFogGraph();

            deque<ProcessOperator> operatorsToProcess = {};
            operatorsToProcess.push_back(ProcessOperator(sinkOperator, nullptr));

            //find the source Node
            vector<FogVertex> sourceNodes;
            const vector<FogVertex> &allVertex = fogGraph.getAllVertex();
            copy_if(allVertex.begin(), allVertex.end(), back_inserter(sourceNodes),
                    [](const FogVertex vertex) {
                        return vertex.ptr->getEntryType() == FogNodeType::Sensor &&
                               vertex.ptr->getRemainingCpuCapacity() > 0;
                    });

            // FIXME: In the absence of link between source operator and sensor node we will pick first sensor with some capacity 
            //  as the source. Refer issue 122.

            if (sourceNodes.empty()) {
                throw "No available source node found in the network to place the operator";
            }

            const FogTopologyEntryPtr &targetSource = sourceNodes[0].ptr;

            // Find the nodes where we can place the operators. First node will be sink and last one will be the target
            // source.
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

            if (candidateNodes.empty()) {
                throw "No path exists between sink and source";
            }

            while (!operatorsToProcess.empty()) {
                ProcessOperator &processOperator = operatorsToProcess.front();

                OperatorPtr &operatorToProcess = processOperator.operatorToProcess;

                if (operatorToProcess->isScheduled()) {
                    break;
                }

                if (operatorToProcess->getOperatorType() == OperatorType::SOURCE_OP) {
                    FogTopologyEntryPtr sourceNode = candidateNodes.back();
                    if (sourceNode->getRemainingCpuCapacity() <= 0){
                        throw "Unable to schedule source operator";
                    }

                    sourceNode->reduceCpuCapacity(1);


                } else {
                    bool scheduled = false;
                    for (FogTopologyEntryPtr node : candidateNodes) {


                        if (node->getRemainingCpuCapacity() > 0) {

                            scheduled = true;
                        }
                    }

                    if (!scheduled) {
                        throw "Unable to schedule operator on the node";
                    }

                }

            }
        };
    };

}
#endif //IOTDB_TOPDOWN_HPP

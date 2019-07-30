#ifndef IOTDB_TOPDOWN_HPP
#define IOTDB_TOPDOWN_HPP

#include <Optimizer/FogPlacementOptimizer.hpp>
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

            deque<ProcessOperator> operatorToProcess = {};
            operatorToProcess.push_back(ProcessOperator(sinkOperator, nullptr));

            //find the source
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
            vector<FogTopologyEntryPtr> candidateNodes = {};

            const FogTopologyEntryPtr &rootNode = fogGraph.getRoot();
            candidateNodes.push_back(rootNode);

            stack<FogTopologyEntryPtr> visited;


            while (!candidateNodes.empty()) {


                const vector<FogEdge> &allEdgesToNode = fogGraph.getAllEdgesToNode(rootNode);

                vector<FogTopologyEntryPtr> childrens = {};

                transform(allEdgesToNode.begin(), allEdgesToNode.end(), back_inserter(childrens),
                          [](FogEdge edge) {
                              return edge.ptr->getSourceNode();
                          });

                if()

                visited.push(rootNode);

            }


            while (!operatorToProcess.empty()) {

            }


        };
    };

}
#endif //IOTDB_TOPDOWN_HPP

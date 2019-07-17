#include "Optimizer/impl/HLF.hpp"
#include <Operators/Operator.hpp>

using namespace iotdb;


ExecutionGraph HLF::prepareExecutionPlan(InputQuery inputQuery, FogTopologyPlan fogTopologyPlan) {

    const OperatorPtr &sinkOperator = inputQuery.getRoot();
    std::vector<OperatorPtr> &childs = sinkOperator->childs;

    const OperatorPtr &sourceOperator();

    const FogTopologyEntryPtr &ptr = fogTopologyPlan.getRootNode();

    OptimizedExecutionGraph executionGraph;

    const ExecutionNodePtr &rootNode = executionGraph.createExecutionNode("sinkOptr", std::to_string(ptr->getId()));


    auto root = sinkOperator;
}


std::map<int, std::vector<OperatorPtr>> buildOperatorMap(OperatorPtr root) {

    std::map<int, std::vector<OperatorPtr>> sortedOperatorMap;

    int level=0;

    while(root->childs) {

    }

};

std::vector<OperatorPtr> getOperatorsOnSameLevel(OperatorPtr root) {

    std::vector<OperatorPtr> childrenOnSameLevel;

    for(OperatorPtr child: root->childs) {

        childrenOnSameLevel.push_back()


    }
}
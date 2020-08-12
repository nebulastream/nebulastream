#include <Optimizer/QueryRefinement/WindowDistributionRefinement.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <string>

namespace NES {

WindowDistributionRefinementPtr WindowDistributionRefinement::create() {
    return std::make_shared<WindowDistributionRefinement>(WindowDistributionRefinement());
}

WindowDistributionRefinement::WindowDistributionRefinement() {}

void WindowDistributionRefinement::execute(std::string queryId) {
    //find paths between
//    std::vector<ExecutionNodePtr> nodes = globalExecutionPlan->getExecutionNodesByQueryId("");
//    for(auto& node : nodes)
//    {
//        QueryPlanPtr plan = node->getQuerySubPlan();
//        plan->getNodeByTpe();
//        std::vector<NodePtr> child = node->getChildren();
//    }
//
//    for (NESTopologyEntryPtr targetSource : sourceNodes) {
//        NES_DEBUG("BottomUpStrategy: Find the path used for performing the placement based on the strategy type for query with id : " << queryId);
//        vector<NESTopologyEntryPtr> path = pathFinder->findPathBetween(targetSource, rootNode);
//        NES_INFO("BottomUpStrategy: Adding system generated operators for query with id : " << queryId);
//        addSystemGeneratedOperators(queryId, path);
//    }

}
}
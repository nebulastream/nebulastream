#include <Nodes/Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/WindowLogicalOperatorNode.hpp>
#include <Optimizer/QueryRefinement/WindowDistributionRefinement.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>

#include <string>

namespace NES {

WindowDistributionRefinementPtr WindowDistributionRefinement::create() {
    return std::make_shared<WindowDistributionRefinement>(WindowDistributionRefinement());
}

WindowDistributionRefinement::WindowDistributionRefinement() : BaseRefinementRule() {}

bool WindowDistributionRefinement::execute(GlobalExecutionPlanPtr globalPlan, std::string queryId) {
    NES_DEBUG("WindowDistributionRefinement::execute for globalPlan=" << globalPlan << " queryId=" << queryId);

    std::vector<ExecutionNodePtr> nodes = globalPlan->getExecutionNodesByQueryId(queryId);
    //rule: if global execution plan consists of more than three nodes, use distributed windowing, else centralized
    bool distributedWindowing = nodes.size() > 3;
    NES_DEBUG("WindowDistributionRefinement::execute: check " << nodes.size() << " nodes");

    //find out if there is a window operator in the query
    for (auto& node : nodes) {
        QueryPlanPtr plan = node->getQuerySubPlan(queryId);
        std::vector<OperatorNodePtr> windowOps = plan->getWindowOperators();
//        std::vector<std::shared_ptr<WindowLogicalOperatorNode>> vec = node->getNodesByType<WindowLogicalOperatorNode>();
        if (windowOps.size() > 0) {

            NES_DEBUG("WindowDistributionRefinement::execute: window operator found on " << windowOps.size() << " nodes");
            for (auto& windowOp : windowOps) {
                NES_DEBUG("WindowDistributionRefinement::execute: window operator found on node " << node->toString() << " windowOp=" << windowOp->toString());
                if (!distributedWindowing) {//centralized window
                    NES_DEBUG("WindowDistributionRefinement::execute: introduce centralized window operator on node " << node->toString() << " windowOp=" << windowOp->toString());
//                    LogicalOperatorNodePtr newWindowOp = windowOp->copyIntoCentralizedWindow();
                    //replace old with new one
//                    windowOp->replace(newWindowOp, windowOp);
                } else {//distributed window
                    NES_DEBUG("WindowDistributionRefinement::execute: introduce distributed window operator on node " << node->toString() << " windowOp=" << windowOp->toString());
//                    LogicalOperatorNodePtr newWindowOp = windowOp->copyIntoDistributedWindow();
//                    //replace old with new one
//                    windowOp->replace(newWindowOp, windowOp);
                    //TODO: here we have to split the operator into the three part operators and deploy them
                }
            }
        } else {
            QueryPlanPtr plan = node->getQuerySubPlan(queryId);
            NES_DEBUG("WindowDistributionRefinement::execute: no window operator found on node " << node->toString() << " plan=" << plan->toString());
        }
    }
    return true;
}
}// namespace NES
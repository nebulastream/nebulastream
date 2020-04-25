#include <Optimizer/QueryRewrite/FilterPushDownRule.hpp>
#include <Nodes/Operators/QueryPlan.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>

namespace NES {

QueryPlanPtr FilterPushDownRule::apply(QueryPlanPtr queryPlanPtr) {

    NES_INFO("FilterPushDownRule: Get all filter nodes in the graph")
    const auto rootOperator = queryPlanPtr->getRootOperator();
    const auto filterOperators = rootOperator->getNodesByType<FilterLogicalOperatorNode>();

    NES_INFO("FilterPushDownRule: Sort all filter nodes in increasing order of the operator id")
//    std::sort(filterOperators.begin(), filterOperators.end());

    for(auto filterOperator: filterOperators) {
        swapAndPushFilter(filterOperator);
    }

    return QueryPlanPtr();
}

RewriteRuleType FilterPushDownRule::getType() {
    return FILTER_PUSH_DOWN;
}

void FilterPushDownRule::swapAndPushFilter(FilterLogicalOperatorNodePtr filterOperator) {

//    OperatorNodePtr children =
//    while ()
}


}


#include <Optimizer/QueryMerger/L0QueryMergerRule.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/GlobalQueryNode.hpp>

namespace NES {

L0QueryMergerRule::L0QueryMergerRule() {}

L0QueryMergerRulePtr L0QueryMergerRule::create() {
    return std::make_shared<L0QueryMergerRule>(L0QueryMergerRule());
}

void L0QueryMergerRule::apply(GlobalQueryPlanPtr globalQueryPlan) {
    std::vector<GlobalQueryNodePtr> globalQueryNodeWithSourceOperator = globalQueryPlan->getAllGlobalQueryNodesWithOperatorType<SourceLogicalOperatorNode>();

}

}// namespace NES
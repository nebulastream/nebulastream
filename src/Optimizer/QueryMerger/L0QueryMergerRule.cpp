#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/QueryMerger/L0QueryMergerRule.hpp>
#include <Plans/Global/Query/GlobalQueryNode.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>

namespace NES {

L0QueryMergerRule::L0QueryMergerRule() {}

L0QueryMergerRulePtr L0QueryMergerRule::create() {
    return std::make_shared<L0QueryMergerRule>(L0QueryMergerRule());
}

void L0QueryMergerRule::apply(std::vector<QueryPlanPtr> queryPlans, GlobalQueryPlanPtr globalQueryPlan) {
    std::vector<GlobalQueryNodePtr> globalQueryNodeWithSourceOperator = globalQueryPlan->getAllGlobalQueryNodesWithOperatorType<SourceLogicalOperatorNode>();

    for (auto& queryPlan : queryPlans) {
        bool merged = false;
        OperatorNodePtr sourceOperator = queryPlan->getOperatorByType<SourceLogicalOperatorNode>()[0];
        std::string targetLogicalSourceName = sourceOperator->as<SourceLogicalOperatorNode>()->getSourceDescriptor()->getStreamName();

        for (auto& globalQuerySourceNode : globalQueryNodeWithSourceOperator) {

            std::vector<OperatorNodePtr> sourceOperators = globalQuerySourceNode->getOperators();
            if (sourceOperators.empty()) {
                //throw exception
            }
            std::string logicalSourceName = sourceOperators[0]->as<SourceLogicalOperatorNode>()->getSourceDescriptor()->getStreamName();

            if (logicalSourceName == targetLogicalSourceName) {
                merged = true;
            }

            if (merged) {
                break;
            }
        }
        if (!merged) {
            globalQueryPlan->addQueryPlan(queryPlan);
            globalQueryNodeWithSourceOperator = globalQueryPlan->getAllGlobalQueryNodesWithOperatorType<SourceLogicalOperatorNode>();
        }
    }
}

bool operatorContainedInGlobalQueryNode(OperatorNodePtr logicalOperator, )



}// namespace NES
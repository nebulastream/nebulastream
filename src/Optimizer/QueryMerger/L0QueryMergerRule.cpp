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

void L0QueryMergerRule::apply(GlobalQueryPlanPtr globalQueryPlan) {

    std::vector<GlobalQueryNodePtr> sinkGQNodes = globalQueryPlan->getAllGlobalQueryNodesWithOperatorType<SinkLogicalOperatorNode>();

    for (int i = 0; i < sinkGQNodes.size(); i++) {
        GlobalQueryNodePtr targetGQN = sinkGQNodes[i];
        for (int j = i + 1; j < sinkGQNodes.size(); j++) {
            GlobalQueryNodePtr hostSinkGQN = sinkGQNodes[j];
            std::vector<NodePtr> targetSourceGQNs = targetGQN->getAllLeafNodes();
            std::vector<NodePtr> hostSourceGQNs = hostSinkGQN->getAllLeafNodes();

            if (areSourceGQNodesEqual(targetSourceGQNs, hostSourceGQNs)) {
                continue;
            }

            for (auto& targetSourceGQN : targetSourceGQNs) {
                bool found = false;
                for (auto& hostSourceGQN : hostSourceGQNs) {
                    if (isGQNMerged(targetSourceGQN->as<GlobalQueryNode>(), hostSourceGQN->as<GlobalQueryNode>())) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    break;
                }
            }
        }
    }
}

bool L0QueryMergerRule::isGQNMerged(GlobalQueryNodePtr targetGQNode, GlobalQueryNodePtr hostGQNode) {

    std::vector<OperatorNodePtr> targetOperators = targetGQNode->getOperators();
    std::vector<OperatorNodePtr> hostOperators = hostGQNode->getOperators();

    if (targetOperators.size() != hostOperators.size()) {
        return false;
    }

    for (auto& targetOperator : targetOperators) {
        bool found = false;
        for (auto& hostOperator : hostOperators) {
            if (targetOperator->equal(hostOperator)) {
                found = true;
                break;
            }
        }
        if (!found) {
            return false;
        }
    }

    for (auto& targetParentGQN : targetGQNode->getParents()) {
        bool found = false;
        for (auto& hostParentGQN : hostGQNode->getParents()) {
            if (isGQNMerged(targetParentGQN->as<GlobalQueryNode>(), hostParentGQN->as<GlobalQueryNode>())) {
                found = true;
                break;
            }
        }
        if (!found) {
            return false;
        }
    }

    for (auto& [queryId, operatorNode] : targetGQNode->getMapOfQueryIdToOperator()) {
        hostGQNode->addQueryAndOperator(queryId, operatorNode);
    }
    return true;
}

bool L0QueryMergerRule::areSourceGQNodesEqual(std::vector<NodePtr> targetSourceGQNs, std::vector<NodePtr> hostSourceGQNs) {

    for (auto& targetSourceGQN : targetSourceGQNs) {
        bool found = false;
        for (auto& hostSourceGQN : hostSourceGQNs) {
            if (targetSourceGQN->as<GlobalQueryNode>()->getId() == hostSourceGQN->as<GlobalQueryNode>()->getId()) {
                found = true;
                break;
            }
        }
        if (!found) {
            return false;
        }
    }
    return true;
}

}// namespace NES
#include <Nodes/Operators/LogicalOperators/MergeLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
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

            std::map<GlobalQueryNodePtr, GlobalQueryNodePtr> targetGQNToHostGQNMap;
            for (auto& targetSourceGQN : targetSourceGQNs) {
                bool found = false;
                for (auto& hostSourceGQN : hostSourceGQNs) {
                    if (checkIfGQNCanMerge(targetSourceGQN->as<GlobalQueryNode>(), hostSourceGQN->as<GlobalQueryNode>(), targetGQNToHostGQNMap)) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    break;
                }
            }

            for (auto& [targetGQN, hostGQN] : targetGQNToHostGQNMap) {
                for (auto& [queryId, operatorNode] : targetGQN->getMapOfQueryIdToOperator()) {
                    hostGQN->addQueryAndOperator(queryId, operatorNode);
                }
            }
        }
    }
}

bool L0QueryMergerRule::checkIfGQNCanMerge(GlobalQueryNodePtr targetGQNode, GlobalQueryNodePtr hostGQNode, std::map<GlobalQueryNodePtr, GlobalQueryNodePtr>& targetGQNToHostGQNMap) {

    //Check if a match was established previously
    if(targetGQNToHostGQNMap.find(targetGQNode) != targetGQNToHostGQNMap.end()){
        return true;
    }

    std::vector<OperatorNodePtr> targetOperators = targetGQNode->getOperators();
    std::vector<OperatorNodePtr> hostOperators = hostGQNode->getOperators();

    if (targetOperators.empty() || hostOperators.empty()) {
        return false;
    }

    if (targetOperators[0]->instanceOf<SinkLogicalOperatorNode>() && hostOperators[0]->instanceOf<SinkLogicalOperatorNode>()) {
        return true;
    } else if (targetOperators[0]->instanceOf<SinkLogicalOperatorNode>() || hostOperators[0]->instanceOf<SinkLogicalOperatorNode>()) {
        return false;
    }

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

    targetGQNToHostGQNMap[targetGQNode] = hostGQNode;

    //Check if all target parents have a matching host parent
    for (auto& targetParentGQN : targetGQNode->getParents()) {
        bool found = false;
        for (auto& hostParentGQN : hostGQNode->getParents()) {
            if (checkIfGQNCanMerge(targetParentGQN->as<GlobalQueryNode>(), hostParentGQN->as<GlobalQueryNode>(), targetGQNToHostGQNMap)) {
                found = true;
                break;
            }
        }
        if (!found) {
            return false;
        }
    }

    //Check if all target children have a matching host child
    for (auto& targetChildGQN : targetGQNode->getChildren()) {
        bool found = false;
        for (auto& hostChildGQN : hostGQNode->getChildren()) {
            if (checkIfGQNCanMerge(targetChildGQN->as<GlobalQueryNode>(), hostChildGQN->as<GlobalQueryNode>(), targetGQNToHostGQNMap)) {
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
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/QueryMerger/L0QueryMergerRule.hpp>
#include <Plans/Global/Query/GlobalQueryNode.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>

namespace NES {

L0QueryMergerRule::L0QueryMergerRule() : processedNodes() {}

L0QueryMergerRulePtr L0QueryMergerRule::create() {
    return std::make_shared<L0QueryMergerRule>(L0QueryMergerRule());
}

bool L0QueryMergerRule::apply(const GlobalQueryPlanPtr& globalQueryPlan) {

    NES_INFO("L0QueryMergerRule: Applying L0 Merging rule to the Global Query Plan");
    std::vector<GlobalQueryNodePtr> globalQueryNodesWithSinkOperators = globalQueryPlan->getAllGlobalQueryNodesWithOperatorType<SinkLogicalOperatorNode>();
    if (globalQueryNodesWithSinkOperators.size() == 1) {
        NES_DEBUG("L0QueryMergerRule: Found only a single query in the global query plan. Skipping the L0 Query Merging.");
        return true;
    }

    NES_DEBUG("L0QueryMergerRule: Iterating over all sink GQNs in the Global Query Plan");
    for (int i = 0; i < globalQueryNodesWithSinkOperators.size(); i++) {
        NES_DEBUG("L0QueryMergerRule: Get the sink GQN and find another sink GQN with same upstream operator chain");
        GlobalQueryNodePtr targetGlobalQueryNodesWithSinkOperator = globalQueryNodesWithSinkOperators[i];
        auto queryIds = targetGlobalQueryNodesWithSinkOperator->getQueryIds();
        if (queryIds.size() > 1) {
            NES_ERROR("L0QueryMergerRule: can't have sink GQN with more than one query");
            throw Exception("L0QueryMergerRule: can't have sink GQN with more than one query");
        }

        NES_DEBUG("L0QueryMergerRule: Get the target query id to find an equivalent operator chains");
        QueryId targetQueryId = queryIds.at(0);

        for (int j = i + 1; j < globalQueryNodesWithSinkOperators.size(); j++) {
            NES_DEBUG("L0QueryMergerRule: Get a host sink GQN for comparison");
            GlobalQueryNodePtr hostGlobalQueryNodesWithSinkOperator = globalQueryNodesWithSinkOperators[j];

            //Get the source GQNs for both target and host sink GQNs
            std::vector<NodePtr> targetGQNsWithSourceOperator = targetGlobalQueryNodesWithSinkOperator->getAllLeafNodes();
            std::vector<NodePtr> hostGQNsWithSourceOperator = hostGlobalQueryNodesWithSinkOperator->getAllLeafNodes();

            //Check if both host and target GQNs have same source GQN nodes
            if (areGQNodesEqual(targetGQNsWithSourceOperator, hostGQNsWithSourceOperator)) {
                NES_DEBUG("L0QueryMergerRule: Skipping execution as both target and host sink GQNs belong to same query plan.");
                continue;
            }

            NES_DEBUG("L0QueryMergerRule: Start with the first target Source GQN and find match for all downstream GQNs.");
            auto& targetSourceGQN = targetGQNsWithSourceOperator[0];

            std::map<GlobalQueryNodePtr, GlobalQueryNodePtr> targetGQNToHostGQNMap;
            bool allTargetGQNFoundMatch = false;
            for (auto& hostGQNWithSourceOperator : hostGQNsWithSourceOperator) {
                NES_TRACE("L0QueryMergerRule: Clear list of previously processed nodes");
                processedNodes.clear();
                if (checkIfGQNCanMerge(targetSourceGQN->as<GlobalQueryNode>(), hostGQNWithSourceOperator->as<GlobalQueryNode>(), targetGQNToHostGQNMap)) {
                    NES_DEBUG("L0QueryMergerRule: Start with the first target Source GQN and find match for all downstream GQNs.");
                    allTargetGQNFoundMatch = true;
                    break;
                }
            }

            if (!allTargetGQNFoundMatch) {
                NES_WARNING("L0QueryMergerRule: Unable to find a completely matching operator chain. Trying next available host GQN.");
                continue;
            }

            NES_DEBUG("L0QueryMergerRule: Found a matching operator chain for performing L0 Merge.");

            NES_TRACE("L0QueryMergerRule: Get all existing GQN for the target Query " << targetQueryId);
            auto gqnListForTargetQuery = globalQueryPlan->getGQNListForQueryId(targetQueryId);

            NES_TRACE("L0QueryMergerRule: Iterate over the pair of matched target and host GQN and perform GQN merge.");
            for (auto& [targetGQN, hostGQN] : targetGQNToHostGQNMap) {
                uint64_t targetGQNId = targetGQN->getId();
                auto found = std::find_if(gqnListForTargetQuery.begin(), gqnListForTargetQuery.end(), [&](const GlobalQueryNodePtr& currentGQN) {
                    return targetGQNId == currentGQN->getId();
                });

                if (found == gqnListForTargetQuery.end()) {
                    NES_ERROR("L0QueryMergerRule: can't find the target GQN node");
                    throw Exception("L0QueryMergerRule: can't find the target GQN node");
                }

                NES_TRACE("L0QueryMergerRule: Iterate over the pair of matched target and host GQN and perform GQN merge.");
                for (auto& [queryId, operatorNode] : targetGQN->getMapOfQueryIdToOperator()) {
                    hostGQN->addQueryAndOperator(queryId, operatorNode);
                }

                NES_TRACE("L0QueryMergerRule: Update the Global query plan to replace target GQN with host GQN.");
                targetGQN->replace(hostGQN);
                gqnListForTargetQuery.erase(found);
                gqnListForTargetQuery.push_back(hostGQN);
            }
            NES_DEBUG("L0QueryMergerRule: Update GQN list for the query " << targetQueryId);
            globalQueryPlan->updateGQNListForQueryId(targetQueryId, gqnListForTargetQuery);
        }
    }
    return true;
}

bool L0QueryMergerRule::checkIfGQNCanMerge(const GlobalQueryNodePtr& targetGQNode, const GlobalQueryNodePtr& hostGQNode, std::map<GlobalQueryNodePtr, GlobalQueryNodePtr>& targetGQNToHostGQNMap) {

    NES_DEBUG("L0QueryMergerRule: Checking if the target GQN: " << targetGQNode->toString() << " and host GQN: " << hostGQNode->toString() << " can be merged");
    processedNodes.push_back(targetGQNode);
    processedNodes.push_back(hostGQNode);
    if (targetGQNode->getId() == 0 && hostGQNode->getId() == 0) {
        NES_DEBUG("L0QueryMergerRule: Skip checking the root of global query plan");
        return true;
    }
    NES_DEBUG("L0QueryMergerRule: Get target and host GQNs operator sets");
    std::vector<OperatorNodePtr> targetOperators = targetGQNode->getOperators();
    std::vector<OperatorNodePtr> hostOperators = hostGQNode->getOperators();
    if (targetOperators.empty() && hostOperators.empty()) {
        NES_WARNING("L0QueryMergerRule: Both target and host operator lists are empty");
        return false;
    }
    if (targetOperators.size() != hostOperators.size()) {
        NES_WARNING("L0QueryMergerRule: target and host operator lists have different size");
        return false;
    }
    if (targetOperators[0]->instanceOf<SinkLogicalOperatorNode>() != hostOperators[0]->instanceOf<SinkLogicalOperatorNode>()) {
        NES_WARNING("L0QueryMergerRule: Matching Sink operator to another operator type.");
        return false;
    }
    if (!targetOperators[0]->instanceOf<SinkLogicalOperatorNode>() && !hostOperators[0]->instanceOf<SinkLogicalOperatorNode>()) {
        NES_DEBUG("L0QueryMergerRule: Comparing target and host operators to find equality");
        for (auto& targetOperator : targetOperators) {
            bool found = false;
            for (auto& hostOperator : hostOperators) {
                if (targetOperator->equal(hostOperator)) {
                    found = true;
                    NES_TRACE("L0QueryMergerRule: Found an equal target and host operator");
                    break;
                }
            }
            if (!found) {
                NES_WARNING("L0QueryMergerRule: unable to find a match for the target operators");
                return false;
            }
        }

        NES_DEBUG("L0QueryMergerRule: Adding matched host GQN to the map");
        targetGQNToHostGQNMap[targetGQNode] = hostGQNode;
    }
    //Check if all target parents have a matching host parent
    for (auto& targetParentGQN : targetGQNode->getParents()) {
        //Check if the node was processed previously
        auto foundExistingMatchForTargetGQN = std::find(processedNodes.begin(), processedNodes.end(), targetParentGQN->as<GlobalQueryNode>());
        if (foundExistingMatchForTargetGQN != processedNodes.end()) {
            NES_TRACE("L0QueryMergerRule: The target GQN " << targetParentGQN->toString() << " was processed already. Skipping.");
            continue;
        }
        bool found = false;
        for (auto& hostParentGQN : hostGQNode->getParents()) {

            //Check if the node was processed previously
            auto foundExistingMatchForHostParentGQN = std::find(processedNodes.begin(), processedNodes.end(), hostParentGQN->as<GlobalQueryNode>());
            if (foundExistingMatchForHostParentGQN != processedNodes.end()) {
                NES_TRACE("L0QueryMergerRule: The target GQN " << hostParentGQN->toString() << " was processed already. Skipping.");
                continue;
            }

            if (checkIfGQNCanMerge(targetParentGQN->as<GlobalQueryNode>(), hostParentGQN->as<GlobalQueryNode>(), targetGQNToHostGQNMap)) {
                found = true;
                break;
            }
        }
        if (!found) {
            NES_WARNING("L0QueryMergerRule: unable to find any match for the target parent GQN: " << targetParentGQN->toString());
            return false;
        }
    }
    //Check if all target children have a matching host child
    for (auto& targetChildGQN : targetGQNode->getChildren()) {
        //Check if the node was processed previously
        auto foundExistingMatchForTargetGQN = std::find(processedNodes.begin(), processedNodes.end(), targetChildGQN->as<GlobalQueryNode>());
        if (foundExistingMatchForTargetGQN != processedNodes.end()) {
            NES_TRACE("L0QueryMergerRule: The target GQN " << targetChildGQN->toString() << " was processed already. Skipping.");
            continue;
        }
        bool found = false;
        for (auto& hostChildGQN : hostGQNode->getChildren()) {
            //Check if the node was processed previously
            auto foundExistingMatchForHostChildGQN = std::find(processedNodes.begin(), processedNodes.end(), hostChildGQN->as<GlobalQueryNode>());
            if (foundExistingMatchForHostChildGQN != processedNodes.end()) {
                NES_TRACE("L0QueryMergerRule: The target GQN " << hostChildGQN->toString() << " was processed already. Skipping.");
                continue;
            }
            if (checkIfGQNCanMerge(targetChildGQN->as<GlobalQueryNode>(), hostChildGQN->as<GlobalQueryNode>(), targetGQNToHostGQNMap)) {
                found = true;
                break;
            }
        }
        if (!found) {
            NES_WARNING("L0QueryMergerRule: unable to find any match for the target child GQN: " << targetChildGQN->toString());
            return false;
        }
    }
    NES_DEBUG("L0QueryMergerRule: Adding matched host GQN to the map");
    return true;
}

bool L0QueryMergerRule::areGQNodesEqual(const std::vector<NodePtr>& targetGQNs, const std::vector<NodePtr>& hostGQNs) {

    NES_DEBUG("L0QueryMergerRule: Checking if the two set of GQNs are equal or not");
    for (auto& targetGQN : targetGQNs) {
        bool found = false;
        for (auto& hostGQN : hostGQNs) {
            if (targetGQN->as<GlobalQueryNode>()->getId() == hostGQN->as<GlobalQueryNode>()->getId()) {
                found = true;
                break;
            }
        }
        if (!found) {
            NES_DEBUG("L0QueryMergerRule: GQN sets are not equal");
            return false;
        }
    }
    NES_DEBUG("L0QueryMergerRule: GQN sets are equal");
    return true;
}

}// namespace NES
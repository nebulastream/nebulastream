#include <Nodes/Operators/OperatorNode.hpp>
#include <Plans/Global/Query/GlobalQueryNode.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger.hpp>
#include <algorithm>

namespace NES {

GlobalQueryPlan::GlobalQueryPlan() {
    root = GlobalQueryNode::createEmpty();
}

GlobalQueryPlanPtr GlobalQueryPlan::create() {
    return std::make_shared<GlobalQueryPlan>(GlobalQueryPlan());
}

void GlobalQueryPlan::addQueryPlan(QueryPlanPtr queryPlan) {

    std::string queryId = queryPlan->getQueryId();
    NES_INFO("GlobalQueryPlan: adding the query plan for query: " << queryId << " to the global query plan.");
    const auto rootOperators = queryPlan->getRootOperators();
    NES_DEBUG("GlobalQueryPlan: adding the root nodes of the query plan for query: " << queryId << " as children to the root node of the global query plan.");
    for (const auto& rootOperator : rootOperators) {
        addNewGlobalOperatorNodeAsChild(root, queryId, rootOperator);
    }
}

//std::vector<SinkLogicalOperatorNodePtr> GlobalQueryPlan::getAllNewSinkOperators() {
//    NES_DEBUG("QueryPlan: Get all New Sink Operators by traversing all the root nodes.");
//    std::vector<SinkLogicalOperatorNodePtr> sinkOperators;
//    for (const auto& rootOperator : rootOperators) {
//        auto sinkOptrs = rootOperator->getNodesByType<SinkLogicalOperatorNode>();
//        NES_DEBUG("QueryPlan: insert all sink operators to the collection");
//        sinkOperators.insert(sinkOperators.end(), sinkOptrs.begin(), sinkOptrs.end());
//    }
//    NES_DEBUG("QueryPlan: Found " << sinkOperators.size() << " sink operators.");
//    return sinkOperators;
//}

void GlobalQueryPlan::addNewGlobalOperatorNodeAsChild(GlobalQueryNodePtr parentNode, std::string queryId, OperatorNodePtr operatorNode) {

    NES_TRACE("GlobalQueryPlan: Checking if the an existing global query node exists for the query operator with id " << operatorNode->getId() << " of query " << queryId);
    std::vector<GlobalQueryNodePtr> globalQueryNodes = getGlobalQueryNodesForQuery(queryId);
    if (!globalQueryNodes.empty()) {
        auto itr = std::find_if(globalQueryNodes.begin(), globalQueryNodes.end(), [&](GlobalQueryNodePtr node) {
            return node->hasOperator(operatorNode);
        });

        if (itr != globalQueryNodes.end()) {
            NES_TRACE("GlobalQueryPlan: Found an existing global query node for the query operator with id " << operatorNode->getId() << " of query " << queryId);
            parentNode->addChild(*itr);
            return;
        }
    }
    NES_TRACE("GlobalQueryPlan: Creating a new global query node for query operator with id " << operatorNode->getId() << " of query " << queryId);
    GlobalQueryNodePtr globalQueryNode = GlobalQueryNode::create(queryId, operatorNode);
    parentNode->addChild(globalQueryNode);
    NES_DEBUG("GlobalQueryPlan: adding new global query node for the children of query operator with id " << operatorNode->getId() << " of query " << queryId);
    std::vector<NodePtr> children = operatorNode->getChildren();
    for (const auto& child : children) {
        addNewGlobalOperatorNodeAsChild(globalQueryNode, queryId, child->as<OperatorNode>());
    }
}

std::vector<GlobalQueryNodePtr> GlobalQueryPlan::getGlobalQueryNodesForQuery(std::string queryId) {
    NES_DEBUG("GlobalQueryPlan: get vector of GlobalQueryNodes for query: " << queryId);

    if (queryToGlobalQueryNodeMap.find(queryId) == queryToGlobalQueryNodeMap.end()) {
        NES_DEBUG("GlobalQueryPlan: get vector of GlobalQueryNodes for query: " << queryId);
        return {};
    } else {
        return queryToGlobalQueryNodeMap[queryId];
    }
}

}// namespace NES
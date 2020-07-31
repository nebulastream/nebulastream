#include <Nodes/Operators/OperatorNode.hpp>
#include <Plans/Global/Query/GlobalQueryNode.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger.hpp>

namespace NES {

GlobalQueryPlan::GlobalQueryPlan() : freeGlobalQueryNodeId(0) {
    root = GlobalQueryNode::createEmpty(getNextFreeId());
}

GlobalQueryPlanPtr GlobalQueryPlan::create() {
    return std::make_shared<GlobalQueryPlan>(GlobalQueryPlan());
}

void GlobalQueryPlan::addQueryPlan(QueryPlanPtr queryPlan) {
    std::string queryId = queryPlan->getQueryId();
    if (queryId.empty()) {
        throw Exception("GlobalQueryPlan: Found query plan without id");
    }
    NES_INFO("GlobalQueryPlan: adding the query plan for query: " << queryId << " to the global query plan.");
    const auto rootOperators = queryPlan->getRootOperators();
    NES_DEBUG("GlobalQueryPlan: adding the root nodes of the query plan for query: " << queryId << " as children to the root node of the global query plan.");
    for (const auto& rootOperator : rootOperators) {
        addUpstreamLogicalOperatorsAsNewGlobalQueryNode(root, queryId, rootOperator);
    }
}

void GlobalQueryPlan::removeQuery(std::string queryId) {
    NES_INFO("GlobalQueryPlan: Remove the query plan for query " << queryId);
    const std::vector<GlobalQueryNodePtr>& globalQueryNodes = getGlobalQueryNodesForQuery(queryId);
    for (GlobalQueryNodePtr globalQueryNode : globalQueryNodes) {
        globalQueryNode->removeQuery(queryId);
        if (globalQueryNode->isEmpty()) {
            globalQueryNodeToQueryMap.erase(globalQueryNode);
        } else {
            std::vector<std::string>& queryIds = globalQueryNodeToQueryMap[globalQueryNode];
            const auto& foundAtLoc = std::find(queryIds.begin(), queryIds.end(), queryId);
            if (foundAtLoc != queryIds.end()) {
                queryIds.erase(foundAtLoc);
                globalQueryNodeToQueryMap[globalQueryNode] = queryIds;
            } else {
                throw Exception("GlobalQueryPlan: Unable to find mapping between Global Query Node and QueryId but a mapping between QueryId and Global Query Node exists.");
            }
        }
    }
    queryToGlobalQueryNodeMap.erase(queryId);
}

void GlobalQueryPlan::addUpstreamLogicalOperatorsAsNewGlobalQueryNode(const GlobalQueryNodePtr& parentNode, std::string queryId, OperatorNodePtr operatorNode) {

    //    NES_TRACE("GlobalQueryPlan: Checking if an existing global query node exists for the operator of query " << queryId);
    //    std::vector<GlobalQueryNodePtr> globalQueryNodes = getGlobalQueryNodesForQuery(queryId);
    //    if (!globalQueryNodes.empty()) {
    //        auto itr = std::find_if(globalQueryNodes.begin(), globalQueryNodes.end(), [&](GlobalQueryNodePtr node) {
    //            return node->hasOperator(operatorNode);
    //        });
    //
    //        if (itr != globalQueryNodes.end()) {
    //            NES_TRACE("GlobalQueryPlan: Found an existing global query node for the operator of query " << queryId);
    //            parentNode->addChild(*itr);
    //            return;
    //        }
    //    }
    NES_TRACE("GlobalQueryPlan: Creating a new global query node for operator of query " << queryId);
    GlobalQueryNodePtr globalQueryNode = GlobalQueryNode::create(getNextFreeId(), queryId, operatorNode);
    queryToGlobalQueryNodeMap[queryId]
    parentNode->addChild(globalQueryNode);
    NES_DEBUG("GlobalQueryPlan: adding new global query node for the children of query operator with id " << operatorNode->getId() << " of query " << queryId);
    std::vector<NodePtr> children = operatorNode->getChildren();
    for (const auto& child : children) {
        addUpstreamLogicalOperatorsAsNewGlobalQueryNode(globalQueryNode, queryId, child->as<OperatorNode>());
    }
}

std::vector<GlobalQueryNodePtr> GlobalQueryPlan::getGlobalQueryNodesForQuery(std::string queryId) {
    NES_DEBUG("GlobalQueryPlan: get vector of GlobalQueryNodes for query: " << queryId);
    if (queryToGlobalQueryNodeMap.find(queryId) == queryToGlobalQueryNodeMap.end()) {
        NES_TRACE("GlobalQueryPlan: Unable to find GlobalQueryNodes for query: " << queryId);
        return std::vector<GlobalQueryNodePtr>();
    } else {
        NES_TRACE("GlobalQueryPlan: Found GlobalQueryNodes for query: " << queryId);
        return queryToGlobalQueryNodeMap[queryId];
    }
}

uint64_t GlobalQueryPlan::getNextFreeId() {
    return freeGlobalQueryNodeId++;
}

}// namespace NES
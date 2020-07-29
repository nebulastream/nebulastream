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

template<class NodeType>
std::vector<GlobalQueryNodePtr> GlobalQueryPlan::getAllNewGlobalQueryNodesWithOperatorType() {
    NES_DEBUG("GlobalQueryPlan: Get all New Global query nodes with specific logical operators");
    std::vector<GlobalQueryNodePtr> vector = getAllGlobalQueryNodesWithOperatorType<NodeType>();
    NES_DEBUG("GlobalQueryPlan: filter all pre-existing Global query nodes");
    std::remove_if(vector.begin(), vector.end(), [](GlobalQueryNodePtr globalQueryNode) {
        return globalQueryNode->wasUpdated();
    });
    return vector;
}

template<class NodeType>
std::vector<GlobalQueryNodePtr> GlobalQueryPlan::getAllGlobalQueryNodesWithOperatorType() {
    NES_DEBUG("GlobalQueryPlan: Get all Global query nodes with specific logical operators");
    std::vector<GlobalQueryNodePtr> vector;
    for (const auto& rootQueryNodes : root->getChildren()) {
        rootQueryNodes->as<GlobalQueryNode>()->getNodesWithTypeHelper<SinkLogicalOperatorNode>(vector);
    }
    return vector;
}

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
        NES_TRACE("GlobalQueryPlan: Unable to find GlobalQueryNodes for query: " << queryId);
        return std::vector<GlobalQueryNodePtr>();
    } else {
        NES_TRACE("GlobalQueryPlan: Found GlobalQueryNodes for query: " << queryId);
        return queryToGlobalQueryNodeMap[queryId];
    }
}

}// namespace NES
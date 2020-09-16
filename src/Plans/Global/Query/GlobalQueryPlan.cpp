#include <Nodes/Operators/OperatorNode.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>

namespace NES {

GlobalQueryPlan::GlobalQueryPlan() : freeGlobalQueryNodeId(0) {
    root = GlobalQueryNode::createEmpty(getNextFreeId());
}

GlobalQueryPlanPtr GlobalQueryPlan::create() {
    return std::make_shared<GlobalQueryPlan>(GlobalQueryPlan());
}

void GlobalQueryPlan::addQueryPlan(QueryPlanPtr queryPlan) {
    NES_ERROR("GlobalQueryPlan: Adding new query plan to the Global query plan");
    QueryId queryId = queryPlan->getQueryId();
    if (queryId == INVALID_QUERY_ID) {
        NES_ERROR("GlobalQueryPlan: Found query plan without query id");
        throw Exception("GlobalQueryPlan: Found query plan without query id");
    }

    if (queryToGlobalQueryNodeMap.find(queryId) != queryToGlobalQueryNodeMap.end()) {
        NES_ERROR("GlobalQueryPlan: Found existing entry for the query Id " << queryId);
        throw Exception("GlobalQueryPlan: Entry for the queryId " + std::to_string(queryId) + " already present. Can't add same query multiple time.");
    }

    NES_INFO("GlobalQueryPlan: adding the query plan for query: " << queryId << " to the global query plan.");
    const auto rootOperators = queryPlan->getRootOperators();
    NES_DEBUG("GlobalQueryPlan: adding the root nodes of the query plan for query: " << queryId << " as children to the root node of the global query plan.");
    for (const auto& rootOperator : rootOperators) {
        addNewGlobalQueryNode(root, queryId, rootOperator);
    }
}

void GlobalQueryPlan::removeQuery(QueryId queryId) {
    NES_INFO("GlobalQueryPlan: Remove the query plan for query " << queryId);
    const std::vector<GlobalQueryNodePtr>& globalQueryNodes = getGQNListForQueryId(queryId);
    for (GlobalQueryNodePtr globalQueryNode : globalQueryNodes) {
        globalQueryNode->removeQuery(queryId);
        if (globalQueryNode->isEmpty()) {
            //TODO: remove the node
            //root->remove(globalQueryNode);
        }
    }
    queryToGlobalQueryNodeMap.erase(queryId);
}

void GlobalQueryPlan::addNewGlobalQueryNode(const GlobalQueryNodePtr& parentNode, const QueryId queryId, const OperatorNodePtr& operatorNode) {

    NES_DEBUG("GlobalQueryPlan: Creating a new global query node for operator of query " << queryId << " and adding it as child to global query node with id " << parentNode->getId());
    GlobalQueryNodePtr newGlobalQueryNode = GlobalQueryNode::create(getNextFreeId(), queryId, operatorNode);
    addGlobalQueryNodeToQuery(queryId, newGlobalQueryNode);
    parentNode->addChild(newGlobalQueryNode);
    NES_DEBUG("GlobalQueryPlan: Creating new global query node for the children of query operator of query " << queryId);
    std::vector<NodePtr> children = operatorNode->getChildren();
    for (const auto& child : children) {
        addNewGlobalQueryNode(newGlobalQueryNode, queryId, child->as<OperatorNode>());
    }
}

std::vector<GlobalQueryNodePtr> GlobalQueryPlan::getGQNListForQueryId(QueryId queryId) {
    NES_DEBUG("GlobalQueryPlan: get vector of GlobalQueryNodes for query: " << queryId);
    if (queryToGlobalQueryNodeMap.find(queryId) == queryToGlobalQueryNodeMap.end()) {
        NES_TRACE("GlobalQueryPlan: Unable to find GlobalQueryNodes for query: " << queryId);
        return std::vector<GlobalQueryNodePtr>();
    } else {
        NES_TRACE("GlobalQueryPlan: Found GlobalQueryNodes for query: " << queryId);
        return queryToGlobalQueryNodeMap[queryId];
    }
}

bool GlobalQueryPlan::addGlobalQueryNodeToQuery(QueryId queryId, GlobalQueryNodePtr globalQueryNode) {
    NES_DEBUG("GlobalQueryPlan: get vector of GlobalQueryNodes for query: " << queryId);
    if (queryToGlobalQueryNodeMap.find(queryId) == queryToGlobalQueryNodeMap.end()) {
        NES_TRACE("GlobalQueryPlan: Unable to find GlobalQueryNodes for query: " << queryId << " . Creating a new entry.");
        queryToGlobalQueryNodeMap[queryId] = {globalQueryNode};
    } else {
        NES_TRACE("GlobalQueryPlan: Found GlobalQueryNodes for query: " << queryId << ". Adding the new global query node to the list.");
        std::vector<GlobalQueryNodePtr> globalQueryNodes = getGQNListForQueryId(queryId);
        globalQueryNodes.push_back(globalQueryNode);
        updateGQNListForQueryId(queryId, globalQueryNodes);
    }
    return true;
}

uint64_t GlobalQueryPlan::getNextFreeId() {
    return freeGlobalQueryNodeId++;
}

bool GlobalQueryPlan::updateGQNListForQueryId(QueryId queryId, std::vector<GlobalQueryNodePtr> globalQueryNodes) {
    if (queryToGlobalQueryNodeMap.find(queryId) == queryToGlobalQueryNodeMap.end()) {
        NES_WARNING("GlobalQueryPlan: unable to find query with id " << queryId << " in the global query plan.");
        return false;
    }
    NES_DEBUG("GlobalQueryPlan: Successfully updated the GQN List for query with id " << queryId);
    queryToGlobalQueryNodeMap[queryId] = globalQueryNodes;
    return true;
}

}// namespace NES
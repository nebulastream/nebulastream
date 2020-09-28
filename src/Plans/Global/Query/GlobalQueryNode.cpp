#include <Nodes/Operators/OperatorNode.hpp>
#include <Plans/Global/Query/GlobalQueryNode.hpp>
#include <algorithm>

namespace NES {

GlobalQueryNode::GlobalQueryNode(uint64_t id) : id(id), scheduled(false), querySetUpdated(false), operatorSetUpdated(false) {}

GlobalQueryNode::GlobalQueryNode(uint64_t id, QueryId queryId, OperatorNodePtr operatorNode) : id(id), scheduled(false), querySetUpdated(true), operatorSetUpdated(true) {
    queryIds.push_back(queryId);
    logicalOperators.push_back(operatorNode);
    queryToOperatorMap[queryId] = operatorNode;
    operatorToQueryMap[operatorNode] = {queryId};
}

GlobalQueryNodePtr GlobalQueryNode::createEmpty(uint64_t id) {
    return std::make_shared<GlobalQueryNode>(GlobalQueryNode(id));
}

GlobalQueryNodePtr GlobalQueryNode::create(uint64_t id, QueryId queryId, OperatorNodePtr operatorNode) {
    return std::make_shared<GlobalQueryNode>(GlobalQueryNode(id, queryId, operatorNode));
}

uint64_t GlobalQueryNode::getId() {
    return id;
}

void GlobalQueryNode::addQueryAndOperator(QueryId queryId, OperatorNodePtr operatorNode) {
    NES_INFO("GlobalQueryNode: Add a new query id " << queryId << " and a logical operator in the global query node " << id);
    if (queryToOperatorMap.find(queryId) != queryToOperatorMap.end()) {
        NES_ERROR("GlobalQueryNode: Can't add query which is already present in the global query node");
        throw Exception("GlobalQueryNode: An entry for query id " + std::to_string(queryId) + " already present in the global query node");
    }
    NES_DEBUG("GlobalQueryNode: Creating new entry for the input query id in the global query node");
    queryToOperatorMap[queryId] = operatorNode;
    queryIds.push_back(queryId);
    NES_INFO("GlobalQueryNode: Marking the query set as updated");
    querySetUpdated = true;
    NES_DEBUG("GlobalQueryNode: Finding if the logical operator already present in the global query node");
    if (hasOperator(operatorNode)) {
        NES_INFO("GlobalQueryNode: The logical operator already present in the global query node. Linking the logical operator and the new query Id");
        std::vector<QueryId>& queryIds = operatorToQueryMap[operatorNode];
        queryIds.push_back(queryId);
    } else {
        NES_INFO("GlobalQueryNode: Creating new entry for the logical operator in the global query node");
        operatorToQueryMap[operatorNode] = {queryId};
        logicalOperators.push_back(operatorNode);
        NES_INFO("GlobalQueryNode: Marking the logical operator set as updated");
        operatorSetUpdated = true;
    }
}

bool GlobalQueryNode::isEmpty() {
    NES_INFO("GlobalQueryNode: Check if the global query node " << id << " is empty");
    return logicalOperators.empty();
}

OperatorNodePtr GlobalQueryNode::hasOperator(OperatorNodePtr operatorNode) {
    NES_INFO("GlobalQueryNode: Check if a similar logical operator present in the global query node " << id);
    auto found = std::find_if(logicalOperators.begin(), logicalOperators.end(), [&](OperatorNodePtr optr) {
        return operatorNode->equal(optr);
    });
    if (found != logicalOperators.end()) {
        NES_INFO("GlobalQueryNode: Returning the similar logical operator present in the global query node " << id);
        return *found;
    }
    NES_INFO("GlobalQueryNode: Found no similar logical operator in the global query node " << id);
    return nullptr;
}

bool GlobalQueryNode::removeQuery(const QueryId queryId) {
    NES_INFO("GlobalQueryNode: Removing entries for the query " << queryId << " from the global query node " << id);
    auto itr = std::find(queryIds.begin(), queryIds.end(), queryId);
    if (itr != queryIds.end()) {
        OperatorNodePtr logicalOperator = queryToOperatorMap[queryId];
        if (!logicalOperator) {
            NES_WARNING("Found no corresponding logical operator for the query " << queryId << " in global query node " << id);
            return false;
        }
        NES_DEBUG("GlobalQueryNode: Find if there are more than 1 query ID associated with the logical operator");
        std::vector<QueryId>& associatedQueries = operatorToQueryMap[logicalOperator];
        if (associatedQueries.size() > 1) {
            NES_DEBUG("GlobalQueryNode: Found " << associatedQueries.size() << " query Ids associated with the logical operator");
            auto itr = std::find(associatedQueries.begin(), associatedQueries.end(), queryId);
            NES_DEBUG("GlobalQueryNode: Removing query id " << queryId << " from the associatedQueries");
            associatedQueries.erase(itr);
        } else {
            NES_DEBUG("GlobalQueryNode: Found only 1 query Ids associated with the logical operator");
            NES_DEBUG("GlobalQueryNode: Removing the entry of logical operator from the global query node " << id);
            operatorToQueryMap.erase(logicalOperator);
            auto iterator = std::find(logicalOperators.begin(), logicalOperators.end(), logicalOperator);
            logicalOperators.erase(iterator);
            NES_DEBUG("GlobalQueryNode: Marking operator set as updated");
            operatorSetUpdated = true;
        }

        NES_DEBUG("GlobalQueryNode: Removing the entry of query id " << queryId << " from the global query node " << id);
        queryIds.erase(itr);
        queryToOperatorMap.erase(queryId);
        NES_DEBUG("GlobalQueryNode: Marking query set as updated");
        querySetUpdated = true;
        return true;
    }
    NES_WARNING("GlobalQueryNode: Unable to find query " << queryId << " in the global query node " << id);
    return false;
}

bool GlobalQueryNode::hasNewUpdate() {
    NES_INFO("GlobalQueryNode: Checking is the global query node with id " << id << " was updated.");
    return querySetUpdated || operatorSetUpdated;
}

void GlobalQueryNode::markAsUpdated() {
    NES_INFO("GlobalQueryNode: Marking the global query node with id " << id << " as updated.");
    querySetUpdated = false;
    operatorSetUpdated = false;
}

const std::string GlobalQueryNode::toString() const {
    std::stringstream operatorString;
    for (auto& logicalOperator : logicalOperators) {
        operatorString << logicalOperator->toString() << " ";
    }
    std::stringstream queryString;
    for (auto& queryId : queryIds) {
        queryString << queryId << " ";
    }
    return "Operators [" + operatorString.str() + "], Queries [" + queryString.str() + "]";
}

std::vector<OperatorNodePtr> GlobalQueryNode::getOperators() {
    return logicalOperators;
}

std::map<QueryId, OperatorNodePtr> GlobalQueryNode::getMapOfQueryIdToOperator() {
    return queryToOperatorMap;
}

bool GlobalQueryNode::hasQuery(QueryId queryId) {
    NES_INFO("GlobalQueryNode: Checking is the global query node has a query with id " << queryId);
    auto found = std::find(queryIds.begin(), queryIds.end(), queryId);
    if (found != queryIds.end()) {
        NES_DEBUG("GlobalQueryNode: Global query node has a query with id " << queryId);
        return true;
    }
    NES_DEBUG("GlobalQueryNode: Global query node does not have a query with id " << queryId);
    return false;
}

const std::vector<QueryId> GlobalQueryNode::getQueryIds() {
    return queryIds;
}

bool GlobalQueryNode::equal(const NodePtr rhs) const {
    if (rhs->instanceOf<GlobalQueryNode>()) {
        return id == rhs->as<GlobalQueryNode>()->getId();
    }
    return false;
}

}// namespace NES
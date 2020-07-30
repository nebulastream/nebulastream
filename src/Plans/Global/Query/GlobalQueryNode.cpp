#include <Nodes/Operators/OperatorNode.hpp>
#include <Plans/Global/Query/GlobalQueryNode.hpp>
#include <algorithm>

namespace NES {

GlobalQueryNode::GlobalQueryNode(uint64_t id) : id(id), scheduled(false), querySetUpdated(false), operatorSetUpdated(false) {}

GlobalQueryNode::GlobalQueryNode(uint64_t id, std::string queryId, OperatorNodePtr operatorNode) : id(id), scheduled(false), querySetUpdated(true), operatorSetUpdated(true) {
    queryIds.push_back(queryId);
    logicalOperators.push_back(operatorNode);
    queryToOperatorMap[queryId] = operatorNode;
    operatorToQueryMap[operatorNode] = {queryId};
}

GlobalQueryNodePtr GlobalQueryNode::createEmpty(uint64_t id) {
    return std::make_shared<GlobalQueryNode>(GlobalQueryNode(id));
}

GlobalQueryNodePtr GlobalQueryNode::create(uint64_t id, std::string queryId, OperatorNodePtr operatorNode) {
    return std::make_shared<GlobalQueryNode>(GlobalQueryNode(id, queryId, operatorNode));
}

uint64_t GlobalQueryNode::getId() {
    return id;
}

void GlobalQueryNode::addQuery(std::string queryId) {
    queryIds.push_back(queryId);
    operatorSetUpdated = true;
}

void GlobalQueryNode::addQueryAndOperator(std::string queryId, OperatorNodePtr operatorNode) {
    queryIds.push_back(queryId);
    logicalOperators.push_back(operatorNode);
    queryToOperatorMap[queryId] = operatorNode;
    operatorSetUpdated = true;
    querySetUpdated = true;
}

bool GlobalQueryNode::isEmpty() {
    return logicalOperators.empty();
}

bool GlobalQueryNode::hasOperator(OperatorNodePtr operatorNode) {
    auto found = std::find_if(logicalOperators.begin(), logicalOperators.end(), [&](OperatorNodePtr optr) {
        return operatorNode->equal(optr);
    });
    return found != logicalOperators.end();
}

bool GlobalQueryNode::removeQuery(const std::string& queryId) {
    auto itr = std::find(queryIds.begin(), queryIds.end(), queryId);
    queryIds.erase(itr);
    OperatorNodePtr logicalOperator = queryToOperatorMap[queryId];
    if (logicalOperator) {
        queryToOperatorMap.erase(queryId);
        querySetUpdated = true;
        std::vector<std::string> associatedQueries = operatorToQueryMap[logicalOperator];
        if (associatedQueries.size() > 1) {
            auto itr = std::find(associatedQueries.begin(), associatedQueries.end(), queryId);
            associatedQueries.erase(itr);
            operatorToQueryMap[logicalOperator] = associatedQueries;
        } else {
            operatorToQueryMap.erase(logicalOperator);
            auto iterator = std::find(logicalOperators.begin(), logicalOperators.end(), logicalOperator);
            logicalOperators.erase(iterator);
            operatorSetUpdated = true;
        }
        return true;
    }
    return false;
}

bool GlobalQueryNode::hasNewUpdate() {
    return querySetUpdated || operatorSetUpdated;
}

void GlobalQueryNode::markAsUpdated() {
    querySetUpdated = false;
    operatorSetUpdated = false;
}

const std::string GlobalQueryNode::toString() const {
    std::stringstream operatorString;
    for (auto& logicalOptr : logicalOperators) {
        operatorString << logicalOptr->toString() << " ";
    }
    std::stringstream queryString;
    for (auto& queryId : queryIds) {
        queryString << queryId << " ";
    }
    return "Operators [" + operatorString.str() + "], Queries [" + queryString.str() + "]";
}

}// namespace NES
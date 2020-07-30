#include <Nodes/Operators/OperatorNode.hpp>
#include <Plans/Global/Query/GlobalQueryNode.hpp>
#include <algorithm>

namespace NES {

GlobalQueryNode::GlobalQueryNode(std::string queryId, OperatorNodePtr operatorNode) : scheduled(false), querySetUpdated(true), operatorSetUpdated(true) {
    queryIds.push_back(queryId);
    logicalOperators.push_back(operatorNode);
    queryToOperatorMap[queryId] = operatorNode;
}

GlobalQueryNodePtr GlobalQueryNode::createEmpty() {
    return std::shared_ptr<GlobalQueryNode>();
}

GlobalQueryNodePtr GlobalQueryNode::create(std::string queryId, OperatorNodePtr operatorNode) {
    return std::shared_ptr<GlobalQueryNode>(GlobalQueryNode(queryId, operatorNode));
}

template<class NodeType>
void GlobalQueryNode::getNodesWithTypeHelper(std::vector<GlobalQueryNodePtr>& foundNodes) {
    if (logicalOperators[0]->instanceOf<NodeType>()) {
        foundNodes.push_back(std::shared_ptr<GlobalQueryNode>(this));
    }
    for (auto& successor : this->children) {
        successor->as<GlobalQueryNode>()->getNodesWithTypeHelper<GlobalQueryNode>(foundNodes);
    }
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
    auto found = std::find_if(logicalOperators.begin(), logicalOperators.end(), [&](OperatorNodePtr optr){
        return operatorNode->equal(optr);
    });
    return found != logicalOperators.end();
}

bool GlobalQueryNode::removeQuery(std::string queryId) {
    auto iterator = std::remove_if(queryIds.begin(), queryIds.end(), queryId);
    queryIds.erase(iterator);
    OperatorNodePtr logicalOperator = queryToOperatorMap[queryId];
    if(logicalOperator){
        queryToOperatorMap.erase(queryId);
        std::vector<std::string> associatedQueries = operatorToQueryMap[logicalOperator];
        if(associatedQueries.size() > 1){
            auto iterator = std::remove_if(associatedQueries.begin(), associatedQueries.end(), queryId);
            associatedQueries.erase(iterator);
            operatorToQueryMap[logicalOperator] = associatedQueries;
        } else{
            operatorToQueryMap.erase(logicalOperator);
            auto iterator = std::remove_if(logicalOperators.begin(), logicalOperators.end(), logicalOperator);
            logicalOperators.erase(iterator);
        }
        return true;
    }
    return false;
}

bool GlobalQueryNode::wasUpdated() {
    return querySetUpdated || operatorSetUpdated;
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
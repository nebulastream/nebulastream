#include <Plans/Global/Query/GlobalQueryMetaData.hpp>
#include <Plans/Global/Query/GlobalQueryNode.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <algorithm>

namespace NES {

GlobalQueryMetaData::GlobalQueryMetaData(uint64_t globalQueryId, std::vector<QueryId> queryIds, std::vector<GlobalQueryNodePtr> sinkGlobalQueryNodes)
    : globalQueryId(globalQueryId), queryIds(queryIds), sinkGlobalQueryNodes(sinkGlobalQueryNodes), deployed(false) {}

GlobalQueryMetaDataPtr GlobalQueryMetaData::create(uint64_t globalQueryId, std::vector<QueryId> queryIds, std::vector<GlobalQueryNodePtr> sinkGlobalQueryNodes) {
    return std::make_shared<GlobalQueryMetaData>(GlobalQueryMetaData(globalQueryId, queryIds, sinkGlobalQueryNodes));
}

void GlobalQueryMetaData::addNewQueryIdAndSinkOperators(QueryId queryId, std::vector<GlobalQueryNodePtr> sinkGlobalQueryNodes) {
    queryIds.push_back(queryId);
    this->sinkGlobalQueryNodes.insert(this->sinkGlobalQueryNodes.end(), sinkGlobalQueryNodes.begin(), sinkGlobalQueryNodes.end());
    markAsNotDeployed();
}

bool GlobalQueryMetaData::removeQueryId(QueryId queryId) {
    auto found = std::find(queryIds.begin(), queryIds.end(), queryId);
    if (found == queryIds.end()) {
        return false;
    }

    for (auto itr = sinkGlobalQueryNodes.begin(); itr != sinkGlobalQueryNodes.end(); itr++) {
        if ((*itr)->hasQuery(queryId)) {
            sinkGlobalQueryNodes.erase(itr--);
        }
    }
    queryIds.erase(found);
    markAsNotDeployed();
    return true;
}

QueryPlanPtr GlobalQueryMetaData::getQueryPlan() {
    QueryPlanPtr queryPlan = QueryPlan::create();
    queryPlan->setQueryId(globalQueryId);
    for (auto sinkGQN : sinkGlobalQueryNodes) {
        OperatorNodePtr rootOperator = sinkGQN->getOperators()[0];
        if (!sinkGQN->getChildren().empty()) {
            appendOperator(rootOperator, sinkGQN->getChildren());
        }
        queryPlan->addRootOperator(rootOperator);
    }
    return queryPlan;
}

void GlobalQueryMetaData::appendOperator(OperatorNodePtr parentOperator, std::vector<NodePtr> childrenGQN) {

    for (auto childGQN : childrenGQN) {
        OperatorNodePtr childOperator = childGQN->as<GlobalQueryNode>()->getOperators()[0];
        parentOperator->addChild(childOperator);
        std::vector<NodePtr> childrenGQNOfChildGQN = childGQN->getChildren();
        if (!childrenGQNOfChildGQN.empty()) {
            appendOperator(childOperator, childrenGQNOfChildGQN);
        }
    }
}

void GlobalQueryMetaData::markAsNotDeployed() {
    this->deployed = false;
}

void GlobalQueryMetaData::markAsDeployed() {
    this->deployed = true;
}

bool GlobalQueryMetaData::empty() {
    return queryIds.empty();
}

bool GlobalQueryMetaData::isDeployed() {
    return deployed;
}

}// namespace NES

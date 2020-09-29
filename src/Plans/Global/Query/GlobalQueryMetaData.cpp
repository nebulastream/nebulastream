#include <Plans/Global/Query/GlobalQueryMetaData.hpp>
#include <Plans/Global/Query/GlobalQueryNode.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/UtilityFunctions.hpp>
#include <algorithm>

namespace NES {

GlobalQueryMetaData::GlobalQueryMetaData(std::set<QueryId> queryIds, std::set<GlobalQueryNodePtr> sinkGlobalQueryNodes)
    : globalQueryId(UtilityFunctions::getNextGlobalQueryId()), queryIds(queryIds), sinkGlobalQueryNodes(sinkGlobalQueryNodes), deployed(false), newMetaData(true) {}

GlobalQueryMetaDataPtr GlobalQueryMetaData::create(std::set<QueryId> queryIds, std::set<GlobalQueryNodePtr> sinkGlobalQueryNodes) {
    return std::make_shared<GlobalQueryMetaData>(GlobalQueryMetaData(queryIds, sinkGlobalQueryNodes));
}

void GlobalQueryMetaData::addNewSinkGlobalQueryNodes(std::set<GlobalQueryNodePtr> sinkGlobalQueryNodes) {
    for (auto sinkGQN : sinkGlobalQueryNodes) {
        queryIds.insert(sinkGQN->getQueryIds()[0]);
        sinkGlobalQueryNodes.insert(sinkGQN);
    }
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
    this->newMetaData = false;
}

bool GlobalQueryMetaData::empty() {
    return queryIds.empty();
}

bool GlobalQueryMetaData::isDeployed() {
    return deployed;
}

bool GlobalQueryMetaData::isNewMetaData() {
    return newMetaData;
}

std::set<GlobalQueryNodePtr> GlobalQueryMetaData::getSinkGlobalQueryNodes() {
    return sinkGlobalQueryNodes;
}

std::set<QueryId> GlobalQueryMetaData::getQueryIds() {
    return queryIds;
}

QueryId GlobalQueryMetaData::getGlobalQueryId() {
    return globalQueryId;
}

void GlobalQueryMetaData::clear() {
    queryIds.clear();
    sinkGlobalQueryNodes.clear();
    markAsNotDeployed();
}

}// namespace NES

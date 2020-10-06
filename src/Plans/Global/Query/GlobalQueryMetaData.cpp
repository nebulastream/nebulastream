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

    NES_DEBUG("GlobalQueryMetaData: Add" << sinkGlobalQueryNodes.size() << "new Global Query Nodes with sink operators");
    for (auto sinkGQN : sinkGlobalQueryNodes) {
        //As GQNs with sink operators do not contain more than 1 query get the query id at 0th index.
        queryIds.insert(sinkGQN->getQueryIds()[0]);
        this->sinkGlobalQueryNodes.insert(sinkGQN);
    }
    //Mark the meta data as updated but not deployed
    markAsNotDeployed();
}

bool GlobalQueryMetaData::removeQueryId(QueryId queryId) {
    auto found = std::find(queryIds.begin(), queryIds.end(), queryId);
    if (found == queryIds.end()) {
        return false;
    }

    auto itr = sinkGlobalQueryNodes.begin();
    while (itr != sinkGlobalQueryNodes.end()) {
        if ((*itr)->hasQuery(queryId)) {
           itr = sinkGlobalQueryNodes.erase(itr);
           continue;
        }
        ++itr;
    }
    queryIds.erase(found);
    markAsNotDeployed();
    return true;
}

QueryPlanPtr GlobalQueryMetaData::getQueryPlan() {
    NES_DEBUG("GlobalQueryMetaData: Prepare the Query Plan based on the Global Query Metadata information");
    QueryPlanPtr queryPlan = QueryPlan::create();
    queryPlan->setQueryId(globalQueryId);
    for (auto sinkGQN : sinkGlobalQueryNodes) {
        OperatorNodePtr rootOperator = sinkGQN->getOperators()[0];
        rootOperator->removeChildren();
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
        childOperator->removeChildren();
        parentOperator->addChild(childOperator);
        std::vector<NodePtr> childrenGQNOfChildGQN = childGQN->getChildren();
        if (!childrenGQNOfChildGQN.empty()) {
            appendOperator(childOperator, childrenGQNOfChildGQN);
        }
    }
}

void GlobalQueryMetaData::markAsNotDeployed() {
    NES_TRACE("GlobalQueryMetaData: Mark the Global Query Metadata as updated but not deployed");
    this->deployed = false;
}

void GlobalQueryMetaData::markAsDeployed() {
    NES_TRACE("GlobalQueryMetaData: Mark the Global Query Metadata as updated but not deployed");
    this->deployed = true;
    this->newMetaData = false;
}

bool GlobalQueryMetaData::empty() {
    NES_TRACE("GlobalQueryMetaData: Mark the Global Query Metadata as updated but not deployed");
    return queryIds.empty();
}

bool GlobalQueryMetaData::isDeployed() {
    NES_TRACE("GlobalQueryMetaData: Checking if Global Query Metadata was already deployed.");
    return deployed;
}

bool GlobalQueryMetaData::isNew() {
    NES_TRACE("GlobalQueryMetaData: Checking if Global Query Metadata was newly constructed.");
    return newMetaData;
}

void GlobalQueryMetaData::setAsOld() {
    NES_TRACE("GlobalQueryMetaData: Marking Global Query Metadata as old post deployment");
    this->newMetaData = false;
}

std::set<GlobalQueryNodePtr> GlobalQueryMetaData::getSinkGlobalQueryNodes() {
    NES_TRACE("GlobalQueryMetaData: Get all Global Query Nodes with sink operators for the current Metadata");
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

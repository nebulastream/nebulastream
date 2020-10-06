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
    NES_DEBUG("GlobalQueryMetaData: Remove the Query Id " << queryId << " and associated Global Query Nodes with sink operators.");
    auto found = std::find(queryIds.begin(), queryIds.end(), queryId);
    if (found == queryIds.end()) {
        NES_WARNING("GlobalQueryMetaData: Unable to find query Id " << queryId << " in Global Query Metadata " << globalQueryId);
        return false;
    }

    NES_TRACE("GlobalQueryMetaData: Remove the Global Query Nodes with sink operators for query " << queryId);
    auto itr = sinkGlobalQueryNodes.begin();
    while (itr != sinkGlobalQueryNodes.end()) {
        if ((*itr)->hasQuery(queryId)) {
            itr = sinkGlobalQueryNodes.erase(itr);
            continue;
        }
        ++itr;
    }
    queryIds.erase(found);
    //Mark the meta data as updated but not deployed
    markAsNotDeployed();
    return true;
}

QueryPlanPtr GlobalQueryMetaData::getQueryPlan() {
    NES_DEBUG("GlobalQueryMetaData: Prepare the Query Plan based on the Global Query Metadata information");
    QueryPlanPtr queryPlan = QueryPlan::create();
    queryPlan->setQueryId(globalQueryId);
    for (auto sinkGQN : sinkGlobalQueryNodes) {
        //Take the first operator as GQN with sink operators only have 1 operator
        OperatorNodePtr rootOperator = sinkGQN->getOperators()[0];
        //clear the previous children assignments
        rootOperator->removeChildren();
        if (!sinkGQN->getChildren().empty()) {
            appendOperator(rootOperator, sinkGQN->getChildren());
        }
        //Add the logical operator as root to the query plan
        queryPlan->addRootOperator(rootOperator);
    }
    return queryPlan;
}

void GlobalQueryMetaData::appendOperator(OperatorNodePtr parentOperator, std::vector<NodePtr> childrenGQN) {
    NES_TRACE("GlobalQueryMetaData: append the operators form the children GQN to the parent logical operator");
    for (auto childGQN : childrenGQN) {
        // We pick the first available operator from the Global Query Node as rest of the operators are same.
        //TODO: this need to be fixed when we use more advanced merging rules for instance merging two filter operators
        // on same input stream but with different predicates.
        OperatorNodePtr childOperator = childGQN->as<GlobalQueryNode>()->getOperators()[0];
        //clear the previous children assignments
        childOperator->removeChildren();
        parentOperator->addChild(childOperator);
        std::vector<NodePtr> childrenGQNOfChildGQN = childGQN->getChildren();
        if (!childrenGQNOfChildGQN.empty()) {
            //recursively call to append children's children
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

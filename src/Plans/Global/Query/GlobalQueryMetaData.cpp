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
    std::vector<OperatorNodePtr> rootOperators;
    std::map<uint64_t, OperatorNodePtr> operatorIdToOperatorMap;

    // We process a Global Query node by extracting its operator and preparing a map of operator id to operator.
    // We push the children operators to the queue of operators to be processed.
    // Every time we encounter an operator, we check in the map if the operator with same id already exists.
    // We use the already existing operator whenever available other wise we create a copy of the operator and add it to the map.
    // We then check the parent operators of the current operator by looking into the parent Global Query Nodes of the current
    // Global Query Node and add them as the parent of the current operator.

    std::deque<NodePtr> globalQueryNodesToProcess{sinkGlobalQueryNodes.begin(), sinkGlobalQueryNodes.end()};
    while (!globalQueryNodesToProcess.empty()) {
        auto gqnToProcess = globalQueryNodesToProcess.front()->as<GlobalQueryNode>();
        globalQueryNodesToProcess.pop_front();
        NES_TRACE("GlobalQueryMetaData: Deserialize operator " << gqnToProcess->toString());
        OperatorNodePtr operatorNode = gqnToProcess->getOperators()[0]->copy();
        uint64_t operatorId = operatorNode->getId();
        if (operatorIdToOperatorMap[operatorId]) {
            NES_TRACE("GlobalQueryMetaData: Operator was already deserialized previously");
            operatorNode = operatorIdToOperatorMap[operatorId];
        } else {
            NES_TRACE("GlobalQueryMetaData: Deserializing the operator and inserting into map");
            operatorIdToOperatorMap[operatorId] = operatorNode;
        }

        for (auto parentNode : gqnToProcess->getParents()) {
            auto parentGQN = parentNode->as<GlobalQueryNode>();
            if (parentGQN->getId() == 0) {
                continue;
            }
            OperatorNodePtr parentOperator = parentGQN->getOperators()[0];
            uint64_t parentOperatorId = parentOperator->getId();
            if (operatorIdToOperatorMap[parentOperatorId]) {
                NES_TRACE("GlobalQueryMetaData: Found the parent operator. Adding as parent to the current operator.");
                parentOperator = operatorIdToOperatorMap[parentOperatorId];
                operatorNode->addParent(parentOperator);
            } else {
                NES_ERROR("GlobalQueryMetaData: unable to find the parent operator. This should not have occurred!");
                return nullptr;
            }
        }

        NES_TRACE("GlobalQueryMetaData: add the child global query nodes for further processing.");
        for (auto childGQN : gqnToProcess->getChildren()) {
            globalQueryNodesToProcess.push_back(childGQN);
        }
    }

    for (auto sinkGlobalQueryNode : sinkGlobalQueryNodes) {
        NES_TRACE("GlobalQueryMetaData: Finding the operator with same id in the map.");
        auto rootOperator = sinkGlobalQueryNode->getOperators()[0]->copy();
        rootOperator = operatorIdToOperatorMap[rootOperator->getId()];
        NES_TRACE("GlobalQueryMetaData: Adding the root operator to the vector of roots for the query plan");
        rootOperators.push_back(rootOperator);
    }
    operatorIdToOperatorMap.clear();
    return QueryPlan::create(globalQueryId, INVALID_QUERY_ID, rootOperators);
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

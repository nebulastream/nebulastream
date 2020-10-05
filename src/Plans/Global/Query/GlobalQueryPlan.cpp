#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Nodes/Operators/OperatorNode.hpp>
#include <Plans/Global/Query/GlobalQueryMetaData.hpp>
#include <Plans/Global/Query/GlobalQueryNode.hpp>
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
    NES_INFO("GlobalQueryPlan: Adding new query plan to the Global query plan");
    QueryId queryId = queryPlan->getQueryId();
    if (queryId == INVALID_QUERY_ID) {
        NES_ERROR("GlobalQueryPlan: Found query plan without query id");
        throw Exception("GlobalQueryPlan: Found query plan without query id");
    }

    if (queryIdToGlobalQueryNodeMap.find(queryId) != queryIdToGlobalQueryNodeMap.end()) {
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
    NES_DEBUG("Removing query information from the meta data");
    QueryId globalQueryId = queryIdToGlobalQueryIdMap[queryId];
    GlobalQueryMetaDataPtr globalQueryMetaData = globalQueryIdToMetaDataMap[globalQueryId];
    globalQueryMetaData->removeQueryId(queryId);
    queryIdToGlobalQueryIdMap.erase(queryId);
    queryIdToGlobalQueryNodeMap.erase(queryId);

    NES_INFO("GlobalQueryPlan: Remove the query plan for query " << queryId);
    const std::vector<GlobalQueryNodePtr>& globalQueryNodes = getGQNListForQueryId(queryId);
    for (GlobalQueryNodePtr globalQueryNode : globalQueryNodes) {
        //Remove the GQN with sink operator from the Global Query Plan
        if (globalQueryNode->getOperators()[0]->instanceOf<SinkLogicalOperatorNode>()) {
            root->remove(globalQueryNode);
        }
        globalQueryNode->removeQuery(queryId);
    }
}

void GlobalQueryPlan::addNewGlobalQueryNode(const GlobalQueryNodePtr& parentNode, const QueryId queryId, const OperatorNodePtr& operatorNode) {

    NES_DEBUG("GlobalQueryPlan: Creating a new global query node for operator of query " << queryId << " and adding it as child to global query node with id " << parentNode->getId());
    GlobalQueryNodePtr newGlobalQueryNode = GlobalQueryNode::create(getNextFreeId(), queryId, operatorNode->copy());
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
    if (queryIdToGlobalQueryNodeMap.find(queryId) == queryIdToGlobalQueryNodeMap.end()) {
        NES_TRACE("GlobalQueryPlan: Unable to find GlobalQueryNodes for query: " << queryId);
        return std::vector<GlobalQueryNodePtr>();
    } else {
        NES_TRACE("GlobalQueryPlan: Found GlobalQueryNodes for query: " << queryId);
        return queryIdToGlobalQueryNodeMap[queryId];
    }
}

bool GlobalQueryPlan::addGlobalQueryNodeToQuery(QueryId queryId, GlobalQueryNodePtr globalQueryNode) {
    NES_DEBUG("GlobalQueryPlan: get vector of GlobalQueryNodes for query: " << queryId);
    if (queryIdToGlobalQueryNodeMap.find(queryId) == queryIdToGlobalQueryNodeMap.end()) {
        NES_TRACE("GlobalQueryPlan: Unable to find GlobalQueryNodes for query: " << queryId << " . Creating a new entry.");
        queryIdToGlobalQueryNodeMap[queryId] = {globalQueryNode};
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
    if (queryIdToGlobalQueryNodeMap.find(queryId) == queryIdToGlobalQueryNodeMap.end()) {
        NES_WARNING("GlobalQueryPlan: unable to find query with id " << queryId << " in the global query plan.");
        return false;
    }
    NES_DEBUG("GlobalQueryPlan: Successfully updated the GQN List for query with id " << queryId);
    queryIdToGlobalQueryNodeMap[queryId] = globalQueryNodes;
    return true;
}

void GlobalQueryPlan::updateGlobalQueryMetaDataMap() {

    checkMetaDataValidity();

    std::vector<GlobalQueryNodePtr> sinkGQNs = getAllGlobalQueryNodesWithOperatorType<SinkLogicalOperatorNode>();

    auto cmp = [](NodePtr a, NodePtr b) {
        return a->as<GlobalQueryNode>()->getId() != b->as<GlobalQueryNode>()->getId();
    };

    std::vector<std::set<NodePtr>> vectorOfGroupedSinkGQNSets;
    for (auto sinkGQN : sinkGQNs) {
        if (queryIdToGlobalQueryIdMap.find(sinkGQN->getQueryIds()[0]) != queryIdToGlobalQueryIdMap.end()) {
            continue;
        }
        std::vector<NodePtr> targetLeafNodes = sinkGQN->getAllLeafNodes();
        std::set<NodePtr> groupedSinkGQNSet{sinkGQN};
        for (auto itr = vectorOfGroupedSinkGQNSets.begin(); itr != vectorOfGroupedSinkGQNSets.end(); itr++) {
            std::set<NodePtr> hostLeafNodes;
            for (auto hostSinkNode : *itr) {
                const std::vector<NodePtr>& leafNodes = hostSinkNode->getAllLeafNodes();
                hostLeafNodes.insert(leafNodes.begin(), leafNodes.end());
            }

            std::set<NodePtr> intersectionSet;
            std::set_intersection(hostLeafNodes.begin(), hostLeafNodes.end(), targetLeafNodes.begin(), targetLeafNodes.end(), std::inserter(intersectionSet, intersectionSet.begin()), cmp);

            if (!intersectionSet.empty()) {
                for (auto hostSinkNode : *itr) {
                    groupedSinkGQNSet.insert(hostSinkNode);
                }
                vectorOfGroupedSinkGQNSets.erase(itr--);
            }
        }
        vectorOfGroupedSinkGQNSets.push_back(groupedSinkGQNSet);
    }

    for (auto groupedSinkGQNs : vectorOfGroupedSinkGQNSets) {
        std::set<QueryId> queryIds;
        std::set<GlobalQueryNodePtr> targetSinkGQNs;
        std::set<NodePtr> targetLeafNodes;
        for (auto sinkGQN : groupedSinkGQNs) {
            targetSinkGQNs.insert(sinkGQN->as<GlobalQueryNode>());
            queryIds.insert(sinkGQN->as<GlobalQueryNode>()->getQueryIds()[0]);

            std::vector<NodePtr> leafNodes = sinkGQN->getAllLeafNodes();
            targetLeafNodes.insert(leafNodes.begin(), leafNodes.end());
        }

        GlobalQueryMetaDataPtr hostGlobalQueryMetaData;
        for (auto [globalQueryId, globalQueryMetaData] : globalQueryIdToMetaDataMap) {
            std::set<GlobalQueryNodePtr> hostSinkNodes = globalQueryMetaData->getSinkGlobalQueryNodes();
            std::set<NodePtr> hostLeafNodes;
            for (auto hostSinkNode : hostSinkNodes) {
                const std::vector<NodePtr>& leafNodes = hostSinkNode->getAllLeafNodes();
                hostLeafNodes.insert(leafNodes.begin(), leafNodes.end());
            }
            std::set<NodePtr> intersectionSet;
            std::set_intersection(targetLeafNodes.begin(), targetLeafNodes.end(), hostLeafNodes.begin(), hostLeafNodes.end(), std::inserter(intersectionSet, intersectionSet.begin()), cmp);

            if (!intersectionSet.empty()) {
                hostGlobalQueryMetaData = globalQueryMetaData;
                break;
            }
        }

        if (hostGlobalQueryMetaData) {
            hostGlobalQueryMetaData->addNewSinkGlobalQueryNodes(targetSinkGQNs);
        } else {
            hostGlobalQueryMetaData = GlobalQueryMetaData::create(queryIds, targetSinkGQNs);
        }

        QueryId globalQueryId = hostGlobalQueryMetaData->getGlobalQueryId();
        globalQueryIdToMetaDataMap[globalQueryId] = hostGlobalQueryMetaData;

        for (auto queryId : queryIds) {
            queryIdToGlobalQueryIdMap[queryId] = globalQueryId;
        }
    }
}

void GlobalQueryPlan::checkMetaDataValidity() {

    auto cmp = [](NodePtr a, NodePtr b) {
        return a->as<GlobalQueryNode>()->getId() != b->as<GlobalQueryNode>()->getId();
    };

    for (auto [globalQueryId, globalQueryMetaData] : globalQueryIdToMetaDataMap) {

        if (globalQueryMetaData->empty() && globalQueryMetaData->isDeployed()) {
            globalQueryIdToMetaDataMap.erase(globalQueryId);
            continue;
        }

        std::set<GlobalQueryNodePtr> sinkGQNs = globalQueryMetaData->getSinkGlobalQueryNodes();
        for (auto itrOuter = sinkGQNs.begin(); itrOuter != sinkGQNs.end(); itrOuter++) {

            std::set<GlobalQueryNodePtr> sinkGQNsWithMatchedSourceGQNs{*itrOuter};
            std::vector<NodePtr> outerLeafNodes = (*itrOuter)->getAllLeafNodes();
            for (auto itrInner = (itrOuter++); itrInner != sinkGQNs.end(); itrInner++) {

                std::vector<NodePtr> innerLeafNodes = (*itrInner)->getAllLeafNodes();
                std::set<NodePtr> intersectionSet;
                std::set_intersection(outerLeafNodes.begin(), outerLeafNodes.end(), innerLeafNodes.begin(), innerLeafNodes.end(), std::inserter(intersectionSet, intersectionSet.begin()), cmp);
                if (!intersectionSet.empty()) {
                    sinkGQNsWithMatchedSourceGQNs.insert(*itrInner);
                }
            }

            if (sinkGQNsWithMatchedSourceGQNs.size() == sinkGQNs.size()) {
                NES_DEBUG("Found no changes to the MetaData information");
                break;
            } else {
                NES_DEBUG("Found MetaData information with non-merged query plans. Clearing MetaData for re-computation.");

                //Get the query ids from the metadata
                std::set<QueryId> queryIds = globalQueryMetaData->getQueryIds();
                for (auto queryId : queryIds) {
                    queryIdToGlobalQueryIdMap.erase(queryId);
                }
                globalQueryMetaData->clear();
            }
        }
    }
}

std::vector<GlobalQueryMetaDataPtr> GlobalQueryPlan::getGlobalQueryMetaDataToDeploy() {
    std::vector<GlobalQueryMetaDataPtr> globalQueryMetaDataToDeploy;

    for (auto [globalQueryId, globalQueryMetaData] : globalQueryIdToMetaDataMap) {
        if (globalQueryMetaData->isDeployed()) {
            continue;
        }

        if (globalQueryMetaData->empty()) {
            globalQueryIdToMetaDataMap.erase(globalQueryMetaData->getGlobalQueryId());
        }
        globalQueryMetaDataToDeploy.push_back(globalQueryMetaData);
    }
    return globalQueryMetaDataToDeploy;
}

QueryId GlobalQueryPlan::getGlobalQueryIdForQuery(QueryId queryId) {
    if (queryIdToGlobalQueryIdMap.find(queryId) != queryIdToGlobalQueryIdMap.end()) {
        return queryIdToGlobalQueryIdMap[queryId];
    }
    return INVALID_QUERY_ID;
}
}// namespace NES
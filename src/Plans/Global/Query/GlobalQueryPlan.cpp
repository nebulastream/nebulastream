/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/OperatorNode.hpp>
#include <Plans/Global/Query/GlobalQueryMetaData.hpp>
#include <Plans/Global/Query/GlobalQueryNode.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>

namespace NES {

GlobalQueryPlan::GlobalQueryPlan() : freeGlobalQueryNodeId(0) { root = GlobalQueryNode::createEmpty(getNextFreeId()); }

GlobalQueryPlanPtr GlobalQueryPlan::create() { return std::make_shared<GlobalQueryPlan>(GlobalQueryPlan()); }

bool GlobalQueryPlan::addQueryPlan(QueryPlanPtr queryPlan) {
    QueryId queryId = queryPlan->getQueryId();
    NES_INFO("GlobalQueryPlan: adding the query plan for query: " << queryId << " to the global query plan.");
    if (queryId == INVALID_QUERY_ID) {
        NES_ERROR("GlobalQueryPlan: Found query plan without query id");
        throw Exception("GlobalQueryPlan: Found query plan without query id");
    }

    if (queryIdToGlobalQueryIdMap.find(queryId) != queryIdToGlobalQueryIdMap.end()) {
        NES_ERROR("GlobalQueryPlan: Found existing entry for the query Id " << queryId);
        throw Exception("GlobalQueryPlan: Entry for the queryId " + std::to_string(queryId)
                        + " already present. Can't add same query multiple time.");
    }

    const auto rootOperators = queryPlan->getRootOperators();
    NES_TRACE("GlobalQueryPlan: adding the root nodes of the query plan for query: "
              << queryId << " as children to the root node of the global query plan.");

    std::map<uint64_t, GlobalQueryNodePtr> operatorToGQNMap;
    std::set<GlobalQueryNodePtr> queryRootGQNs;
    for (const auto& rootOperator : rootOperators) {
        auto queryRootGQNode = GlobalQueryNode::create(getNextFreeId(), rootOperator->copy());
        queryRootGQNs.insert(queryRootGQNode);
        operatorToGQNMap[rootOperator->getId()] = queryRootGQNode;
        root->addChild(queryRootGQNode);

        std::vector<NodePtr> children = rootOperator->getChildren();
        std::deque<NodePtr> nodesToProcess{children.begin(), children.end()};

        while (!nodesToProcess.empty()) {

            auto operatorToProcess = nodesToProcess.front()->as<OperatorNode>();
            nodesToProcess.pop_front();
            uint64_t operatorId = operatorToProcess->getId();
            if (operatorToGQNMap[operatorId]) {
                continue;
            } else {
                auto gqNode = GlobalQueryNode::create(getNextFreeId(), operatorToProcess->copy());
                operatorToGQNMap[operatorToProcess->getId()] = gqNode;
            }

            //Add the link to parent GQN nodes
            for (auto parent : operatorToProcess->getParents()) {

                auto parentOperator = parent->as<OperatorNode>();
                auto parentOperatorId = parentOperator->getId();
                if (operatorToGQNMap[parentOperatorId]) {
                    operatorToGQNMap[operatorId]->addParent(operatorToGQNMap[parentOperatorId]);
                } else {
                    NES_ERROR("GlobalQueryPlan: unable to find the parent global query node. This should not have occurred!");
                    return false;
                }
            }

            for (auto child : operatorToProcess->getChildren()) {
                nodesToProcess.emplace_back(child);
            }
        }
    }

    auto globalQueryMetadata = GlobalQueryMetaData::create({queryId}, queryRootGQNs);
    return updateGlobalQueryMetadata(globalQueryMetadata);
}

void GlobalQueryPlan::removeQuery(QueryId queryId) {
    NES_DEBUG("Removing query information from the meta data");
    GlobalQueryId globalQueryId = queryIdToGlobalQueryIdMap[queryId];
    GlobalQueryMetaDataPtr globalQueryMetaData = globalQueryIdToMetaDataMap[globalQueryId];
    globalQueryMetaData->removeQueryId(queryId);
    queryIdToGlobalQueryIdMap.erase(queryId);
}

uint64_t GlobalQueryPlan::getNextFreeId() { return freeGlobalQueryNodeId++; }

std::vector<GlobalQueryMetaDataPtr> GlobalQueryPlan::getGlobalQueryMetaDataToDeploy() {
    NES_DEBUG("GlobalQueryPlan: Get the Global MetaData to be deployed.");
    std::vector<GlobalQueryMetaDataPtr> globalQueryMetaDataToDeploy;
    NES_TRACE("GlobalQueryPlan: Iterate over the Map with global query metadata.");
    for (auto [globalQueryId, globalQueryMetaData] : globalQueryIdToMetaDataMap) {
        if (globalQueryMetaData->isDeployed()) {
            NES_TRACE("GlobalQueryPlan: Skipping! found already deployed query meta data.");
            continue;
        }
        globalQueryMetaDataToDeploy.push_back(globalQueryMetaData);
    }
    NES_DEBUG("GlobalQueryPlan: Found " << globalQueryMetaDataToDeploy.size() << "  Global MetaData to be deployed.");
    return globalQueryMetaDataToDeploy;
}

GlobalQueryId GlobalQueryPlan::getGlobalQueryIdForQuery(QueryId queryId) {
    NES_DEBUG("GlobalQueryPlan: Get the Global Query Id for the query " << queryId);
    if (queryIdToGlobalQueryIdMap.find(queryId) != queryIdToGlobalQueryIdMap.end()) {
        return queryIdToGlobalQueryIdMap[queryId];
    }
    NES_WARNING("GlobalQueryPlan: Unable to find Global Query Id for the query " << queryId);
    return INVALID_GLOBAL_QUERY_ID;
}

bool GlobalQueryPlan::updateGlobalQueryMetadata(GlobalQueryMetaDataPtr globalQueryMetaData) {
    NES_INFO("GlobalQueryPlan: updating the global query metadata information");
    auto globalQueryId = globalQueryMetaData->getGlobalQueryId();
    globalQueryIdToMetaDataMap[globalQueryId] = globalQueryMetaData;

    NES_TRACE("GlobalQueryPlan: Updating the Query Id to Global Query Id map");
    for (auto [queryId, sinkGQNs] : globalQueryMetaData->getQueryIdToSinkGQNMap()) {
        queryIdToGlobalQueryIdMap[queryId] = globalQueryId;
    }
    return true;
}

void GlobalQueryPlan::removeEmptyMetaData() {
    NES_INFO("GlobalQueryPlan: remove empty metadata information.");
    for (auto [globalQueryId, globalQueryMetaData] : globalQueryIdToMetaDataMap) {
        if ((globalQueryMetaData->isDeployed() || globalQueryMetaData->isNew()) && globalQueryMetaData->isEmpty()) {
            NES_TRACE("GlobalQueryPlan: Removing! found an empty query meta data.");
            globalQueryIdToMetaDataMap.erase(globalQueryMetaData->getGlobalQueryId());
        }
    }
}

std::vector<GlobalQueryMetaDataPtr> GlobalQueryPlan::getAllGlobalQueryMetaData() {
    NES_INFO("GlobalQueryPlan: Get all metadata information");
    std::vector<GlobalQueryMetaDataPtr> globalQueryMetaDataToDeploy;
    NES_TRACE("GlobalQueryPlan: Iterate over the Map with global query metadata.");
    for (auto [globalQueryId, globalQueryMetaData] : globalQueryIdToMetaDataMap) {
        globalQueryMetaDataToDeploy.push_back(globalQueryMetaData);
    }
    NES_TRACE("GlobalQueryPlan: Found " << globalQueryMetaDataToDeploy.size() << "  Global MetaData.");
    return globalQueryMetaDataToDeploy;
}
}// namespace NES
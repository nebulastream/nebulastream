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
#include <Plans/Global/Query/GlobalQueryNode.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryMetaData.hpp>
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

    if (queryIdToSharedQueryIdMap.find(queryId) != queryIdToSharedQueryIdMap.end()) {
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

    auto sharedQueryMetadata = SharedQueryMetaData::create({queryId}, queryRootGQNs);
    return updateSharedQueryMetadata(sharedQueryMetadata);
}

void GlobalQueryPlan::removeQuery(QueryId queryId) {
    NES_DEBUG("Removing query information from the meta data");
    SharedQueryId sharedQueryId = queryIdToSharedQueryIdMap[queryId];
    SharedQueryMetaDataPtr sharedQueryMetaData = sharedQueryIdToMetaDataMap[sharedQueryId];
    sharedQueryMetaData->removeQueryId(queryId);
    queryIdToSharedQueryIdMap.erase(queryId);
}

uint64_t GlobalQueryPlan::getNextFreeId() { return freeGlobalQueryNodeId++; }

std::vector<SharedQueryMetaDataPtr> GlobalQueryPlan::getSharedQueryMetaDataToDeploy() {
    NES_DEBUG("GlobalQueryPlan: Get the Global MetaData to be deployed.");
    std::vector<SharedQueryMetaDataPtr> sharedQueryMetaDataToDeploy;
    NES_TRACE("GlobalQueryPlan: Iterate over the Map with global query metadata.");
    for (auto [sharedQueryId, sharedQueryMetaData] : sharedQueryIdToMetaDataMap) {
        if (sharedQueryMetaData->isDeployed()) {
            NES_TRACE("GlobalQueryPlan: Skipping! found already deployed query meta data.");
            continue;
        }
        sharedQueryMetaDataToDeploy.push_back(sharedQueryMetaData);
    }
    NES_DEBUG("GlobalQueryPlan: Found " << sharedQueryMetaDataToDeploy.size() << "  Shared Query MetaData to be deployed.");
    return sharedQueryMetaDataToDeploy;
}

SharedQueryId GlobalQueryPlan::getSharedQueryIdForQuery(QueryId queryId) {
    NES_DEBUG("GlobalQueryPlan: Get the Global Query Id for the query " << queryId);
    if (queryIdToSharedQueryIdMap.find(queryId) != queryIdToSharedQueryIdMap.end()) {
        return queryIdToSharedQueryIdMap[queryId];
    }
    NES_WARNING("GlobalQueryPlan: Unable to find Global Query Id for the query " << queryId);
    return INVALID_SHARED_QUERY_ID;
}

bool GlobalQueryPlan::updateSharedQueryMetadata(SharedQueryMetaDataPtr sharedQueryMetaData) {
    NES_INFO("GlobalQueryPlan: updating the shared query metadata information");
    auto sharedQueryId = sharedQueryMetaData->getSharedQueryId();
    sharedQueryIdToMetaDataMap[sharedQueryId] = sharedQueryMetaData;

    NES_TRACE("GlobalQueryPlan: Updating the Query Id to Shared Query Id map");
    for (auto [queryId, sinkGQNs] : sharedQueryMetaData->getQueryIdToSinkGQNMap()) {
        queryIdToSharedQueryIdMap[queryId] = sharedQueryId;
    }
    return true;
}

void GlobalQueryPlan::removeEmptySharedQueryMetaData() {
    NES_INFO("GlobalQueryPlan: remove empty metadata information.");
    for (auto [sharedQueryId, sharedQueryMetaData] : sharedQueryIdToMetaDataMap) {
        if ((sharedQueryMetaData->isDeployed() || sharedQueryMetaData->isNew()) && sharedQueryMetaData->isEmpty()) {
            NES_TRACE("GlobalQueryPlan: Removing! found an empty query meta data.");
            sharedQueryIdToMetaDataMap.erase(sharedQueryMetaData->getSharedQueryId());
        }
    }
}

std::vector<SharedQueryMetaDataPtr> GlobalQueryPlan::getAllSharedQueryMetaData() {
    NES_INFO("GlobalQueryPlan: Get all metadata information");
    std::vector<SharedQueryMetaDataPtr> sharedQueryMetaDataToDeploy;
    NES_TRACE("GlobalQueryPlan: Iterate over the Map of shared query metadata.");
    for (auto [sharedQueryId, sharedQueryMetaData] : sharedQueryIdToMetaDataMap) {
        sharedQueryMetaDataToDeploy.push_back(sharedQueryMetaData);
    }
    NES_TRACE("GlobalQueryPlan: Found " << sharedQueryMetaDataToDeploy.size() << "  Shared Query MetaData.");
    return sharedQueryMetaDataToDeploy;
}
}// namespace NES
/*
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

#include <cstdint>
#include <API/Schema.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <GRPC/CoordinatorRPCServer.hpp>
#include <Services/ReplicationService.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Topology/TopologyNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>

namespace NES {

ReplicationService::ReplicationService(NesCoordinatorPtr coordinatorPtr) : coordinatorPtr(std::move(coordinatorPtr)) {}

int ReplicationService::getCurrentEpochBarrier(uint64_t queryId, uint64_t epoch) const {
    std::unique_lock lock(replicationServiceMutex);
    auto pairEpochTimestamp = this->queryIdToCurrentEpochBarrierMap.at(queryId);
    if (pairEpochTimestamp.first == epoch) {
        return pairEpochTimestamp.second;
    }
    else {
        return -1;
    }
}

void ReplicationService::saveEpochBarrier(uint64_t queryId, uint64_t epochBarrier) const {
    std::pair<uint64_t, uint64_t> newPairEpochTimestamp;
    auto iterator = queryIdToCurrentEpochBarrierMap.find(queryId);
    if (iterator != queryIdToCurrentEpochBarrierMap.end()) {
        newPairEpochTimestamp = std::pair(iterator->first + 1, epochBarrier);
    }
    else {
        newPairEpochTimestamp = std::pair(0, epochBarrier);
    }
    queryIdToCurrentEpochBarrierMap[queryId] = newPairEpochTimestamp;
}

std::vector<SourceLogicalOperatorNodePtr> ReplicationService::getLogicalSources(uint64_t queryId) const {
    auto globalQueryPlan = this->coordinatorPtr->getGlobalQueryPlan();
    auto sharedQueryPlan = globalQueryPlan->getSharedQueryPlan(queryId);
    if (!sharedQueryPlan) {
        return  sharedQueryPlan->getQueryPlan()->getSourceOperators();
    }
    NES_ERROR("ReplicationService: no shared query plan found for the queryId " << queryId);
    return {};
}

std::vector<TopologyNodePtr> ReplicationService::getPhysicalSources(SourceLogicalOperatorNodePtr logicalSource) const {
    SourceDescriptorPtr sourceDescriptor = logicalSource->getSourceDescriptor();
    auto streamName = sourceDescriptor->getSchema()->getStreamNameQualifier();
    return this->coordinatorPtr->getStreamCatalog()->getSourceNodesForLogicalStream(streamName);
}

bool ReplicationService::notifyEpochTermination(uint64_t epochBarrier, uint64_t queryId) const {
    std::unique_lock lock(replicationServiceMutex);
    this->saveEpochBarrier(queryId, epochBarrier);
    NES_DEBUG("ReplicationService: send timestamp " << epochBarrier << "to sources with queryId " << queryId);
    std::vector<SourceLogicalOperatorNodePtr> sources = getLogicalSources(queryId);
    if (!sources.empty()) {
        for (auto& sourceOperator : sources) {
            std::vector<TopologyNodePtr> sourceLocations = getPhysicalSources(sourceOperator);
            if (!sourceLocations.empty()) {
                bool success = false;
                for (auto& sourceLocation : sourceLocations) {
                    auto workerRpcClient = std::make_shared<WorkerRPCClient>();
                    success =
                        workerRpcClient->injectEpochBarrier(epochBarrier, queryId, sourceLocation->getIpAddress());
                    NES_ASSERT(success, false);
                    NES_DEBUG("ReplicationService: success=" << success);
                }
                return success;
            } else {
                NES_ERROR("ReplicationService: no physical sources found for the queryId " << queryId);
            }
        }
    } else {
        NES_ERROR("ReplicationService: no sources found for the queryId " << queryId);
    }
    return false;
}

}
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

int ReplicationService::getcurrentEpochBarrierForGivenQueryAndEpoch(uint64_t queryId, uint64_t epoch) const {
    std::unique_lock lock(replicationServiceMutex);
    auto pairEpochTimestamp = this->queryIdToCurrentEpochBarrierMap.at(queryId);
    if (pairEpochTimestamp.first == epoch) {
        return pairEpochTimestamp.second;
    }
    else {
        return -1;
    }
}

bool ReplicationService::notifyEpochTermination(uint64_t epochBarrier, uint64_t querySubPlanId) {
    std::unique_lock lock(replicationServiceMutex);
    std::vector<SourceLogicalOperatorNodePtr> sources;
    uint64_t queryId = this->coordinatorPtr->getNodeEngine()->getQueryIdFromSubQueryId(querySubPlanId);
    auto iterator = queryIdToCurrentEpochBarrierMap.find(queryId);
    std::pair<uint64_t, uint64_t> newPairEpochTimestamp;
    if (iterator != queryIdToCurrentEpochBarrierMap.end()) {
        newPairEpochTimestamp = std::pair(iterator->first + 1, epochBarrier);
    }
    else {
        newPairEpochTimestamp = std::pair(0, epochBarrier);
    }
    queryIdToCurrentEpochBarrierMap[queryId] = newPairEpochTimestamp;
    NES_DEBUG("NesCoordinator::propagatePunctuation send timestamp " << epochBarrier << "to sources with queryId " << queryId);
    auto queryPlan = this->coordinatorPtr->getGlobalQueryPlan()->getSharedQueryPlan(queryId)->getQueryPlan();
    uint64_t curQueryId = queryPlan->getQueryId();
    auto logicalSinkOperators = queryPlan->getSinkOperators();
    if (!sources.empty()) {
        for (auto& sourceOperator : sources) {
            SourceDescriptorPtr sourceDescriptor = sourceOperator->getSourceDescriptor();
            auto streamName = sourceDescriptor->getSchema()->getStreamNameQualifier();
            std::vector<TopologyNodePtr> sourceLocations = this->coordinatorPtr->getStreamCatalog() ->getSourceNodesForLogicalStream(streamName);
            if (!sourceLocations.empty()) {
                for (auto& sourceLocation : sourceLocations) {
                    auto workerRpcClient = std::make_shared<WorkerRPCClient>();
                    bool success =
                        workerRpcClient->injectEpochBarrier(epochBarrier, curQueryId, sourceLocation->getIpAddress());
                    NES_DEBUG("NesWorker::propagatePunctuation success=" << success);
                    return success;
                }
            } else {
                NES_ERROR("NesCoordinator::propagatePunctuation: no physical sources found for the queryId " << queryId);
            }
        }
    } else {
        NES_ERROR("NesCoordinator::propagatePunctuation: no sources found for the queryId " << queryId);
    }
    return false;
}

}
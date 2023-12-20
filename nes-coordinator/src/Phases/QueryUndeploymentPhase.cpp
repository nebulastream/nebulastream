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

#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Exceptions/ExecutionNodeNotFoundException.hpp>
#include <Exceptions/QueryUndeploymentException.hpp>
#include <Exceptions/RPCQueryUndeploymentException.hpp>
#include <Exceptions/RpcException.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <Phases/QueryUndeploymentPhase.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <WorkerRPCService.grpc.pb.h>
#include <utility>

namespace NES {

QueryUndeploymentPhase::QueryUndeploymentPhase(const TopologyPtr& topology, const GlobalExecutionPlanPtr& globalExecutionPlan)
    : topology(topology), globalExecutionPlan(globalExecutionPlan), workerRPCClient(WorkerRPCClient::create()) {
    NES_DEBUG("QueryUndeploymentPhase()");
}

QueryUndeploymentPhasePtr QueryUndeploymentPhase::create(const TopologyPtr& topology,
                                                         const GlobalExecutionPlanPtr& globalExecutionPlan) {
    return std::make_shared<QueryUndeploymentPhase>(QueryUndeploymentPhase(topology, globalExecutionPlan));
}

void QueryUndeploymentPhase::execute(const SharedQueryId sharedQueryId, SharedQueryPlanStatus sharedQueryPlanStatus) {
    NES_DEBUG("QueryUndeploymentPhase::stopAndUndeployQuery : queryId= {}", sharedQueryId);

    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(sharedQueryId);

    if (executionNodes.empty()) {
        NES_ERROR("QueryUndeploymentPhase: Unable to find ExecutionNodes where the query {} is deployed", sharedQueryId);
        throw Exceptions::ExecutionNodeNotFoundException("Unable to find ExecutionNodes where the query "
                                                         + std::to_string(sharedQueryId) + " is deployed");
    }

    NES_DEBUG("QueryUndeploymentPhase:removeQuery: stop query");
    //todo 3916: changed method's signature since the return value was always true; invokes NES_THROW_RUNTIME_ERROR
    //todo: what kind of error is NES_THROW_RUNTIME_ERROR?
    stopQuery(sharedQueryId, executionNodes, sharedQueryPlanStatus);

    NES_DEBUG("QueryUndeploymentPhase:removeQuery: undeploy query  {}", sharedQueryId);
    undeployQuery(sharedQueryId, executionNodes);

    const std::map<uint64_t, uint32_t>& resourceMap = globalExecutionPlan->getMapOfWorkerIdToOccupiedResource(sharedQueryId);

    for (const auto [id, resourceAmount] : resourceMap) {
        NES_TRACE("QueryUndeploymentPhase: Releasing {} resources for the node {}", resourceAmount, id);
        topology->releaseResources(id, resourceAmount);
    }

    if (!globalExecutionPlan->removeQuerySubPlans(sharedQueryId)) {
        throw Exceptions::QueryUndeploymentException(sharedQueryId,
                                                     "Failed to remove query subplans for the query "
                                                         + std::to_string(sharedQueryId) + '.');
    }
}

void QueryUndeploymentPhase::stopQuery(QueryId sharedQueryId,
                                       const std::vector<ExecutionNodePtr>& executionNodes,
                                       SharedQueryPlanStatus sharedQueryPlanStatus) {
    NES_DEBUG("QueryUndeploymentPhase:markQueryForStop queryId= {}", sharedQueryId);
    //NOTE: the uncommented lines below have to be activated for async calls
    std::map<CompletionQueuePtr, uint64_t> completionQueues;
    std::map<CompletionQueuePtr, uint64_t> mapQueueToExecutionNodeId;

    for (auto&& executionNode : executionNodes) {
        CompletionQueuePtr queueForExecutionNode = std::make_shared<CompletionQueue>();
        const auto& nesNode = executionNode->getTopologyNode();
        auto ipAddress = nesNode->getIpAddress();
        auto grpcPort = nesNode->getGrpcPort();
        std::string rpcAddress = ipAddress + ":" + std::to_string(grpcPort);
        NES_DEBUG("QueryUndeploymentPhase::markQueryForStop at execution node with id={} and IP={}",
                  executionNode->getId(),
                  rpcAddress);

        Runtime::QueryTerminationType queryTerminationType;

        if (SharedQueryPlanStatus::UPDATED == sharedQueryPlanStatus || SharedQueryPlanStatus::STOPPED == sharedQueryPlanStatus) {
            queryTerminationType = Runtime::QueryTerminationType::HardStop;
        } else if (SharedQueryPlanStatus::FAILED == sharedQueryPlanStatus) {
            queryTerminationType = Runtime::QueryTerminationType::Failure;
        } else {
            NES_ERROR("Unhandled request type {}", std::string(magic_enum::enum_name(sharedQueryPlanStatus)));
            NES_NOT_IMPLEMENTED();
        }

        workerRPCClient->stopQueryAsync(rpcAddress, sharedQueryId, queryTerminationType, queueForExecutionNode);
        completionQueues[queueForExecutionNode] = 1;
        mapQueueToExecutionNodeId[queueForExecutionNode] = executionNode->getId();
    }

    // activate below for async calls
    try {
        workerRPCClient->checkAsyncResult(completionQueues, RpcClientModes::Stop);
        NES_DEBUG("QueryDeploymentPhase: Finished stopping execution plan for query with Id {}", sharedQueryId);
    } catch (Exceptions::RpcException& e) {
        std::vector<uint64_t> failedRpcsExecutionNodeIds;
        for (const auto& failedRpcInfo : e.getFailedCalls()) {
            failedRpcsExecutionNodeIds.push_back(mapQueueToExecutionNodeId.at(failedRpcInfo.completionQueue));
        }
        throw Exceptions::RPCQueryUndeploymentException(e.what(), failedRpcsExecutionNodeIds, RpcClientModes::Stop);
    }
}

void QueryUndeploymentPhase::undeployQuery(QueryId sharedQueryId, const std::vector<ExecutionNodePtr>& executionNodes) {
    NES_DEBUG("QueryUndeploymentPhase::undeployQuery queryId= {}", sharedQueryId);

    std::map<CompletionQueuePtr, uint64_t> completionQueues;
    std::map<CompletionQueuePtr, uint64_t> mapQueueToExecutionNodeId;

    for (const ExecutionNodePtr& executionNode : executionNodes) {
        CompletionQueuePtr queueForExecutionNode = std::make_shared<CompletionQueue>();

        const auto& nesNode = executionNode->getTopologyNode();
        auto ipAddress = nesNode->getIpAddress();
        auto grpcPort = nesNode->getGrpcPort();
        std::string rpcAddress = ipAddress + ":" + std::to_string(grpcPort);
        NES_DEBUG("QueryUndeploymentPhase::undeployQuery query at execution node with id={} and IP={}",
                  executionNode->getId(),
                  rpcAddress);
        workerRPCClient->unregisterQueryAsync(rpcAddress, sharedQueryId, queueForExecutionNode);
        completionQueues[queueForExecutionNode] = 1;
        mapQueueToExecutionNodeId[queueForExecutionNode] = executionNode->getId();
    }
    try {
        workerRPCClient->checkAsyncResult(completionQueues, RpcClientModes::Unregister);
        NES_DEBUG("QueryDeploymentPhase: Finished stopping execution plan for query with Id {}", sharedQueryId);
    } catch (Exceptions::RpcException& e) {
        std::vector<uint64_t> failedRpcsExecutionNodeIds;
        for (const auto& failedRpcInfo : e.getFailedCalls()) {
            failedRpcsExecutionNodeIds.push_back(mapQueueToExecutionNodeId.at(failedRpcInfo.completionQueue));
        }
        throw Exceptions::RPCQueryUndeploymentException(e.what(), failedRpcsExecutionNodeIds, RpcClientModes::Unregister);
    }
}
}// namespace NES
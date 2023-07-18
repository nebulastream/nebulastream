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

#include <Exceptions/ExecutionNodeNotFoundException.hpp>
#include <Exceptions/QueryUndeploymentException.hpp>
#include <Exceptions/RPCQueryUndeploymentException.hpp>
#include <Exceptions/RpcException.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <Phases/QueryUndeploymentPhase.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <WorkerRPCService.grpc.pb.h>
#include <utility>

namespace NES {

QueryUndeploymentPhase::QueryUndeploymentPhase(TopologyPtr topology,
                                               GlobalExecutionPlanPtr globalExecutionPlan,
                                               WorkerRPCClientPtr workerRpcClient)
    : topology(std::move(topology)), globalExecutionPlan(std::move(globalExecutionPlan)),
      workerRPCClient(std::move(workerRpcClient)) {
    NES_DEBUG("QueryUndeploymentPhase()");
}

QueryUndeploymentPhasePtr QueryUndeploymentPhase::create(TopologyPtr topology,
                                                         GlobalExecutionPlanPtr globalExecutionPlan,
                                                         WorkerRPCClientPtr workerRpcClient) {
    return std::make_shared<QueryUndeploymentPhase>(
        QueryUndeploymentPhase(std::move(topology), std::move(globalExecutionPlan), std::move(workerRpcClient)));
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
    bool successStop = stopQuery(sharedQueryId, executionNodes, sharedQueryPlanStatus);
    //todo 3916: this query returns always true if there is no exception, so the following block is astually obsolete
    if (successStop) {
        NES_DEBUG("QueryUndeploymentPhase:removeQuery: stop query successful for  {}", sharedQueryId);
    } else {
        NES_ERROR("QueryUndeploymentPhase:removeQuery: stop query failed for {}", sharedQueryId);
        // XXX: C++2a: Modernize to std::format("Failed to stop the query {}.", queryId)
        throw Exceptions::QueryUndeploymentException(sharedQueryId, "Failed to stop the query " + std::to_string(sharedQueryId) + '.');
    }

    NES_DEBUG("QueryUndeploymentPhase:removeQuery: undeploy query  {}", sharedQueryId);
    bool successUndeploy = undeployQuery(sharedQueryId, executionNodes);
    if (successUndeploy) {
        NES_DEBUG("QueryUndeploymentPhase:removeQuery: undeploy query successful");
    } else {
        NES_ERROR("QueryUndeploymentPhase:removeQuery: undeploy query failed");
        // XXX: C++2a: Modernize to std::format("Failed to stop the query {}.", queryId)
        throw Exceptions::QueryUndeploymentException(sharedQueryId, "Failed to stop the query " + std::to_string(sharedQueryId) + '.');
    }

    const std::map<uint64_t, uint32_t>& resourceMap = globalExecutionPlan->getMapOfTopologyNodeIdToOccupiedResource(sharedQueryId);

    for (auto entry : resourceMap) {
        NES_TRACE("QueryUndeploymentPhase: Releasing {} resources for the node {}", entry.second, entry.first);
        topology->increaseResources(entry.first, entry.second);
    }

    if (!globalExecutionPlan->removeQuerySubPlans(sharedQueryId)) {
        throw Exceptions::QueryUndeploymentException(sharedQueryId,
                                                     "Failed to remove query subplans for the query " + std::to_string(sharedQueryId)
                                                         + '.');
    }
}

bool QueryUndeploymentPhase::stopQuery(QueryId queryId,
                                       const std::vector<ExecutionNodePtr>& executionNodes,
                                       SharedQueryPlanStatus sharedQueryPlanStatus) {
    NES_DEBUG("QueryUndeploymentPhase:markQueryForStop queryId= {}", queryId);
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

        if (SharedQueryPlanStatus::Updated == sharedQueryPlanStatus || SharedQueryPlanStatus::Stopped == sharedQueryPlanStatus) {
            queryTerminationType = Runtime::QueryTerminationType::HardStop;
        } else if (SharedQueryPlanStatus::Failed == sharedQueryPlanStatus) {
            queryTerminationType = Runtime::QueryTerminationType::Failure;
        } else {
            NES_ERROR("Unhandled request type {}", std::string(magic_enum::enum_name(sharedQueryPlanStatus)));
            NES_NOT_IMPLEMENTED();
        }

        //todo 3916: this function als oalways returns true if there is no exception thrown
        bool success = workerRPCClient->stopQueryAsync(rpcAddress, queryId, queryTerminationType, queueForExecutionNode);
        if (success) {
            NES_DEBUG("QueryUndeploymentPhase::markQueryForStop {} to {} successful", queryId, rpcAddress);
        } else {
            NES_ERROR("QueryUndeploymentPhase::markQueryForStop  {} to {} failed", queryId, rpcAddress);
            return false;
        }
        completionQueues[queueForExecutionNode] = 1;
        mapQueueToExecutionNodeId[queueForExecutionNode] = executionNode->getId();
    }

    // activate below for async calls
    try {
        //todo 3916: this function returns always true: change that
        bool result = workerRPCClient->checkAsyncResult(completionQueues, RpcClientModes::Stop);
        NES_DEBUG("QueryDeploymentPhase: Finished stopping execution plan for query with Id {} success={}", queryId, result);
    } catch (Exceptions::RpcException& e) {
        std::vector<uint64_t> failedRpcsExecutionNodeIds;
        for (const auto& failedRpcInfo : e.getFailedCalls()) {
            failedRpcsExecutionNodeIds.push_back(mapQueueToExecutionNodeId.at(failedRpcInfo.completionQueue));
        }
        throw Exceptions::RPCQueryUndeploymentException(e.what(), failedRpcsExecutionNodeIds, RpcClientModes::Stop);
    }
    //todo 3916: what about this return value?
    return true;
}

bool QueryUndeploymentPhase::undeployQuery(QueryId queryId, const std::vector<ExecutionNodePtr>& executionNodes) {
    NES_DEBUG("QueryUndeploymentPhase::undeployQuery queryId= {}", queryId);

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
        bool success = workerRPCClient->unregisterQueryAsync(rpcAddress, queryId, queueForExecutionNode);
        if (success) {
            NES_DEBUG("QueryUndeploymentPhase::undeployQuery query {} to {} successful", queryId, rpcAddress);
        } else {
            NES_ERROR("QueryUndeploymentPhase::undeployQuery {} to {} failed", queryId, rpcAddress);
            return false;
        }

        completionQueues[queueForExecutionNode] = 1;
        mapQueueToExecutionNodeId[queueForExecutionNode] = executionNode->getId();
    }
    try {
        bool result = workerRPCClient->checkAsyncResult(completionQueues, RpcClientModes::Unregister);
        NES_DEBUG("QueryDeploymentPhase: Finished stopping execution plan for query with Id {} success={}", queryId, result);
        return result;
    } catch (Exceptions::RpcException& e) {
        std::vector<uint64_t> failedRpcsExecutionNodeIds;
        for (const auto& failedRpcInfo : e.getFailedCalls()) {
            failedRpcsExecutionNodeIds.push_back(mapQueueToExecutionNodeId.at(failedRpcInfo.completionQueue));
        }
        throw Exceptions::RPCQueryUndeploymentException(e.what(), failedRpcsExecutionNodeIds, RpcClientModes::Unregister);
    }
    return false;
}
}// namespace NES
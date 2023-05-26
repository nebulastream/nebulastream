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

#include <Exceptions/QueryUndeploymentException.hpp>
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
    NES_DEBUG2("QueryUndeploymentPhase()");
}

QueryUndeploymentPhasePtr QueryUndeploymentPhase::create(TopologyPtr topology,
                                                         GlobalExecutionPlanPtr globalExecutionPlan,
                                                         WorkerRPCClientPtr workerRpcClient) {
    return std::make_shared<QueryUndeploymentPhase>(
        QueryUndeploymentPhase(std::move(topology), std::move(globalExecutionPlan), std::move(workerRpcClient)));
}

void QueryUndeploymentPhase::execute(const QueryId queryId, SharedQueryPlanStatus sharedQueryPlanStatus) {
    NES_DEBUG2("QueryUndeploymentPhase::stopAndUndeployQuery : queryId= {}", queryId);

    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    if (executionNodes.empty()) {
        NES_ERROR2("QueryUndeploymentPhase: Unable to find ExecutionNodes where the query {} is deployed", queryId);
        throw QueryUndeploymentException("Unable to find ExecutionNodes where the query " + std::to_string(queryId)
                                         + " is deployed");
    }

    NES_DEBUG2("QueryUndeploymentPhase:removeQuery: stop query");
    bool successStop = stopQuery(queryId, executionNodes, sharedQueryPlanStatus);
    if (successStop) {
        NES_DEBUG2("QueryUndeploymentPhase:removeQuery: stop query successful for  {}", queryId);
    } else {
        NES_ERROR2("QueryUndeploymentPhase:removeQuery: stop query failed for {}", queryId);
        // XXX: C++2a: Modernize to std::format("Failed to stop the query {}.", queryId)
        throw QueryUndeploymentException("Failed to stop the query " + std::to_string(queryId) + '.');
    }

    NES_DEBUG2("QueryUndeploymentPhase:removeQuery: undeploy query  {}", queryId);
    bool successUndeploy = undeployQuery(queryId, executionNodes);
    if (successUndeploy) {
        NES_DEBUG2("QueryUndeploymentPhase:removeQuery: undeploy query successful");
    } else {
        NES_ERROR2("QueryUndeploymentPhase:removeQuery: undeploy query failed");
        // XXX: C++2a: Modernize to std::format("Failed to stop the query {}.", queryId)
        throw QueryUndeploymentException("Failed to stop the query " + std::to_string(queryId) + '.');
    }

    const std::map<uint64_t, uint32_t>& resourceMap = globalExecutionPlan->getMapOfTopologyNodeIdToOccupiedResource(queryId);

    for (auto entry : resourceMap) {
        NES_TRACE2("QueryUndeploymentPhase: Releasing {} resources for the node {}", entry.second, entry.first);
        topology->increaseResources(entry.first, entry.second);
    }

    if (!globalExecutionPlan->removeQuerySubPlans(queryId)) {
        throw QueryUndeploymentException("Failed to remove query subplans for the query " + std::to_string(queryId) + '.');
    }
}

bool QueryUndeploymentPhase::stopQuery(QueryId queryId,
                                       const std::vector<ExecutionNodePtr>& executionNodes,
                                       SharedQueryPlanStatus sharedQueryPlanStatus) {
    NES_DEBUG2("QueryUndeploymentPhase:markQueryForStop queryId= {}", queryId);
    //NOTE: the uncommented lines below have to be activated for async calls
    std::map<CompletionQueuePtr, uint64_t> completionQueues;

    for (auto&& executionNode : executionNodes) {
        CompletionQueuePtr queueForExecutionNode = std::make_shared<CompletionQueue>();
        const auto& nesNode = executionNode->getTopologyNode();
        auto ipAddress = nesNode->getIpAddress();
        auto grpcPort = nesNode->getGrpcPort();
        std::string rpcAddress = ipAddress + ":" + std::to_string(grpcPort);
        NES_DEBUG2("QueryUndeploymentPhase::markQueryForStop at execution node with id={} and IP={}",
                   executionNode->getId(),
                   rpcAddress);

        Runtime::QueryTerminationType queryTerminationType;

        if (SharedQueryPlanStatus::Updated == sharedQueryPlanStatus || SharedQueryPlanStatus::Stopped == sharedQueryPlanStatus) {
            queryTerminationType = Runtime::QueryTerminationType::HardStop;
        } else if (SharedQueryPlanStatus::Failed == sharedQueryPlanStatus) {
            queryTerminationType = Runtime::QueryTerminationType::Failure;
        } else {
            NES_ERROR2("Unhandled request type {}", std::string(magic_enum::enum_name(sharedQueryPlanStatus)));
            NES_NOT_IMPLEMENTED();
        }

        bool success = workerRPCClient->stopQueryAsync(rpcAddress, queryId, queryTerminationType, queueForExecutionNode);
        if (success) {
            NES_DEBUG2("QueryUndeploymentPhase::markQueryForStop {} to {} successful", queryId, rpcAddress);
        } else {
            NES_ERROR2("QueryUndeploymentPhase::markQueryForStop  {} to {} failed", queryId, rpcAddress);
            return false;
        }
        completionQueues[queueForExecutionNode] = 1;
    }

    // activate below for async calls
    bool result = workerRPCClient->checkAsyncResult(completionQueues, RpcClientModes::Stop);
    NES_DEBUG2("QueryDeploymentPhase: Finished stopping execution plan for query with Id {} success={}", queryId, result);
    return true;
}

bool QueryUndeploymentPhase::undeployQuery(QueryId queryId, const std::vector<ExecutionNodePtr>& executionNodes) {
    NES_DEBUG2("QueryUndeploymentPhase::undeployQuery queryId= {}", queryId);

    std::map<CompletionQueuePtr, uint64_t> completionQueues;

    for (const ExecutionNodePtr& executionNode : executionNodes) {
        CompletionQueuePtr queueForExecutionNode = std::make_shared<CompletionQueue>();

        const auto& nesNode = executionNode->getTopologyNode();
        auto ipAddress = nesNode->getIpAddress();
        auto grpcPort = nesNode->getGrpcPort();
        std::string rpcAddress = ipAddress + ":" + std::to_string(grpcPort);
        NES_DEBUG2("QueryUndeploymentPhase::undeployQuery query at execution node with id={} and IP={}",
                   executionNode->getId(),
                   rpcAddress);
        //        bool success = workerRPCClient->unregisterQuery(rpcAddress, queryId);
        bool success = workerRPCClient->unregisterQueryAsync(rpcAddress, queryId, queueForExecutionNode);
        if (success) {
            NES_DEBUG2("QueryUndeploymentPhase::undeployQuery query {} to {} successful", queryId, rpcAddress);
        } else {
            NES_ERROR2("QueryUndeploymentPhase::undeployQuery {} to {} failed", queryId, rpcAddress);
            return false;
        }

        completionQueues[queueForExecutionNode] = 1;
    }
    bool result = workerRPCClient->checkAsyncResult(completionQueues, RpcClientModes::Unregister);
    NES_DEBUG2("QueryDeploymentPhase: Finished stopping execution plan for query with Id {} success={}", queryId, result);
    return result;
}
}// namespace NES
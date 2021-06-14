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

#include <Exceptions/QueryDeploymentException.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <Phases/QueryDeploymentPhase.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <WorkerRPCService.grpc.pb.h>
#include <utility>
namespace NES {

QueryDeploymentPhase::QueryDeploymentPhase(GlobalExecutionPlanPtr globalExecutionPlan, WorkerRPCClientPtr workerRpcClient)
    : workerRPCClient(std::move(workerRpcClient)), globalExecutionPlan(std::move(globalExecutionPlan)) {
    NES_DEBUG("QueryDeploymentPhase()");
}

QueryDeploymentPhasePtr QueryDeploymentPhase::create(GlobalExecutionPlanPtr globalExecutionPlan,
                                                     WorkerRPCClientPtr workerRpcClient) {
    return std::make_shared<QueryDeploymentPhase>(
        QueryDeploymentPhase(std::move(globalExecutionPlan), std::move(workerRpcClient)));
}

bool QueryDeploymentPhase::execute(QueryId queryId) {
    NES_DEBUG("QueryDeploymentPhase: deploy the query");

    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);
    if (executionNodes.empty()) {
        NES_ERROR("QueryDeploymentPhase: Unable to find ExecutionNodes to be deploy the query " << queryId);
        throw QueryDeploymentException(queryId,
                                       "QueryDeploymentPhase: Unable to find ExecutionNodes to be deploy the query "
                                           + std::to_string(queryId));
    }

    bool successDeploy = deployQuery(queryId, executionNodes);
    if (successDeploy) {
        NES_DEBUG("QueryDeploymentPhase: deployment for query " + std::to_string(queryId) + " successful");
    } else {
        NES_ERROR("QueryDeploymentPhase: Failed to deploy query " << queryId);
        throw QueryDeploymentException(queryId, "QueryDeploymentPhase: Failed to deploy query " + std::to_string(queryId));
    }

    NES_DEBUG("QueryService: start query");
    bool successStart = startQuery(queryId, executionNodes);
    if (successStart) {
        NES_DEBUG("QueryDeploymentPhase: Successfully started deployed query " << queryId);
    } else {
        NES_ERROR("QueryDeploymentPhase: Failed to start the deployed query " << queryId);
        throw QueryDeploymentException(queryId, "QueryDeploymentPhase: Failed to deploy query " + std::to_string(queryId));
    }
    return true;
}

bool QueryDeploymentPhase::deployQuery(QueryId queryId, const std::vector<ExecutionNodePtr>& executionNodes) {
    NES_DEBUG("QueryDeploymentPhase::deployQuery queryId=" << queryId);
    std::map<CompletionQueuePtr, uint64_t> completionQueues;
    for (const ExecutionNodePtr& executionNode : executionNodes) {
        NES_DEBUG("QueryDeploymentPhase::registerQueryInNodeEngine serialize id=" << executionNode->getId());
        std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
        if (querySubPlans.empty()) {
            NES_WARNING("QueryDeploymentPhase : unable to find query sub plan with id " << queryId);
            return false;
        }

        CompletionQueuePtr queueForExecutionNode = std::make_shared<CompletionQueue>();

        const auto& nesNode = executionNode->getTopologyNode();
        auto ipAddress = nesNode->getIpAddress();
        auto grpcPort = nesNode->getGrpcPort();
        std::string rpcAddress = ipAddress + ":" + std::to_string(grpcPort);
        NES_DEBUG("QueryDeploymentPhase:deployQuery: " << queryId << " to " << rpcAddress);

        for (auto& querySubPlan : querySubPlans) {
            //enable this for sync calls
            //bool success = workerRPCClient->registerQuery(rpcAddress, querySubPlan);
            bool success = workerRPCClient->registerQueryAsync(rpcAddress, querySubPlan, queueForExecutionNode);
            if (success) {
                NES_DEBUG("QueryDeploymentPhase:deployQuery: " << queryId << " to " << rpcAddress << " successful");
            } else {
                NES_ERROR("QueryDeploymentPhase:deployQuery: " << queryId << " to " << rpcAddress << "  failed");
                return false;
            }
        }
        completionQueues[queueForExecutionNode] = querySubPlans.size();
    }
    bool result = workerRPCClient->checkAsyncResult(completionQueues, Register);
    NES_DEBUG("QueryDeploymentPhase: Finished deploying execution plan for query with Id " << queryId << " success=" << result);
    return result;
}

bool QueryDeploymentPhase::startQuery(QueryId queryId, const std::vector<ExecutionNodePtr>& executionNodes) {
    NES_DEBUG("QueryDeploymentPhase::startQuery queryId=" << queryId);
    //TODO: check if one queue can be used among multiple connections
    std::map<CompletionQueuePtr, uint64_t> completionQueues;

    for (const ExecutionNodePtr& executionNode : executionNodes) {
        CompletionQueuePtr queueForExecutionNode = std::make_shared<CompletionQueue>();

        const auto& nesNode = executionNode->getTopologyNode();
        auto ipAddress = nesNode->getIpAddress();
        auto grpcPort = nesNode->getGrpcPort();
        std::string rpcAddress = ipAddress + ":" + std::to_string(grpcPort);
        NES_DEBUG("QueryDeploymentPhase::startQuery at execution node with id=" << executionNode->getId()

                                                                                << " and IP=" << ipAddress);
        //enable this for sync calls
        //bool success = workerRPCClient->startQuery(rpcAddress, queryId);
        bool success = workerRPCClient->startQueryAsyn(rpcAddress, queryId, queueForExecutionNode);
        if (success) {
            NES_DEBUG("QueryDeploymentPhase::startQuery " << queryId << " to " << rpcAddress << " successful");
        } else {
            NES_ERROR("QueryDeploymentPhase::startQuery " << queryId << " to " << rpcAddress << "  failed");
            return false;
        }
        completionQueues[queueForExecutionNode] = 1;
    }

    bool result = workerRPCClient->checkAsyncResult(completionQueues, Start);
    NES_DEBUG("QueryDeploymentPhase: Finished starting execution plan for query with Id " << queryId << " success=" << result);
    return result;
}

}// namespace NES
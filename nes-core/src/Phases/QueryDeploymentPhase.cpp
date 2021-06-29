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

#include <Exceptions/QueryDeploymentException.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <Phases/QueryDeploymentPhase.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Services/QueryCatalogService.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <WorkerRPCService.grpc.pb.h>
#include <utility>
namespace NES {

QueryDeploymentPhase::QueryDeploymentPhase(GlobalExecutionPlanPtr globalExecutionPlan,
                                           WorkerRPCClientPtr workerRpcClient,
                                           QueryCatalogServicePtr catalogService)
    : workerRPCClient(std::move(workerRpcClient)), globalExecutionPlan(std::move(globalExecutionPlan)),
      queryCatalogService(std::move(catalogService)) {}

QueryDeploymentPhasePtr QueryDeploymentPhase::create(GlobalExecutionPlanPtr globalExecutionPlan,
                                                     WorkerRPCClientPtr workerRpcClient,
                                                     QueryCatalogServicePtr catalogService) {
    return std::make_shared<QueryDeploymentPhase>(
        QueryDeploymentPhase(std::move(globalExecutionPlan), std::move(workerRpcClient), std::move(catalogService)));
}

bool QueryDeploymentPhase::execute(SharedQueryPlanPtr sharedQueryPlan) {
    NES_DEBUG("QueryDeploymentPhase: deploy the query");

    auto sharedQueryId = sharedQueryPlan->getSharedQueryId();

    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(sharedQueryId);
    if (executionNodes.empty()) {
        NES_ERROR("QueryDeploymentPhase: Unable to find ExecutionNodes to be deploy the shared query " << sharedQueryId);
        throw QueryDeploymentException(sharedQueryId,
                                       "QueryDeploymentPhase: Unable to find ExecutionNodes to be deploy the shared query "
                                           + std::to_string(sharedQueryId));
    }

    //Remove the old mapping of the shared query plan
    if (SharedQueryPlanStatus::Updated == sharedQueryPlan->getStatus()) {
        queryCatalogService->removeSharedQueryPlanMapping(sharedQueryId);
    }

    //Reset all sub query plan metadata in the catalog
    for (auto& queryId : sharedQueryPlan->getQueryIds()) {
        queryCatalogService->resetSubQueryMetaData(queryId);
        queryCatalogService->mapSharedQueryPlanId(sharedQueryId, queryId);
    }

    //Add sub query plan metadata in the catalog
    for (auto& executionNode : executionNodes) {
        auto workerId = executionNode->getId();
        auto subQueryPlans = executionNode->getQuerySubPlans(sharedQueryId);
        for (auto& subQueryPlan : subQueryPlans) {
            QueryId querySubPlanId = subQueryPlan->getQuerySubPlanId();
            for (auto& queryId : sharedQueryPlan->getQueryIds()) {
                queryCatalogService->addSubQueryMetaData(queryId, querySubPlanId, workerId);
            }
        }
    }

    //Mark queries as deployed
    for (auto& queryId : sharedQueryPlan->getQueryIds()) {
        queryCatalogService->updateQueryStatus(queryId, QueryStatus::Deployed, "");
    }

    bool successDeploy = deployQuery(sharedQueryId, executionNodes);
    if (successDeploy) {
        NES_DEBUG("QueryDeploymentPhase: deployment for shared query " + std::to_string(sharedQueryId) + " successful");
    } else {
        NES_ERROR("QueryDeploymentPhase: Failed to deploy shared query " << sharedQueryId);
        throw QueryDeploymentException(sharedQueryId,
                                       "QueryDeploymentPhase: Failed to deploy shared query " + std::to_string(sharedQueryId));
    }

    //Mark queries as running
    for (auto& queryId : sharedQueryPlan->getQueryIds()) {
        queryCatalogService->updateQueryStatus(queryId, QueryStatus::Running, "");
    }

    NES_DEBUG("QueryService: start query");
    bool successStart = startQuery(sharedQueryId, executionNodes);
    if (successStart) {
        NES_DEBUG("QueryDeploymentPhase: Successfully started deployed shared query " << sharedQueryId);
    } else {
        NES_ERROR("QueryDeploymentPhase: Failed to start the deployed shared query " << sharedQueryId);
        throw QueryDeploymentException(sharedQueryId,
                                       "QueryDeploymentPhase: Failed to deploy query " + std::to_string(sharedQueryId));
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
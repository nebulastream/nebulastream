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

#include <Catalogs/UDF/JavaUDFDescriptor.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Exceptions/QueryDeploymentException.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <Operators/LogicalOperators/OpenCLLogicalOperatorNode.hpp>
#include <Phases/QueryDeploymentPhase.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Services/QueryCatalogService.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <cpr/cpr.h>
#include <nlohmann/json.hpp>
#include <utility>

namespace NES {

QueryDeploymentPhase::QueryDeploymentPhase(GlobalExecutionPlanPtr globalExecutionPlan,
                                           WorkerRPCClientPtr workerRpcClient,
                                           QueryCatalogServicePtr catalogService,
                                           bool accelerateJavaUDFs,
                                           std::string accelerationServiceURL)
    : workerRPCClient(std::move(workerRpcClient)), globalExecutionPlan(std::move(globalExecutionPlan)),
      queryCatalogService(std::move(catalogService)), accelerateJavaUDFs(accelerateJavaUDFs),
      accelerationServiceURL(std::move(accelerationServiceURL)) {}

QueryDeploymentPhasePtr
QueryDeploymentPhase::create(GlobalExecutionPlanPtr globalExecutionPlan,
                             WorkerRPCClientPtr workerRpcClient,
                             QueryCatalogServicePtr catalogService,
                             const Configurations::CoordinatorConfigurationPtr& coordinatorConfiguration) {
    return std::make_shared<QueryDeploymentPhase>(
        QueryDeploymentPhase(std::move(globalExecutionPlan),
                             std::move(workerRpcClient),
                             std::move(catalogService),
                             coordinatorConfiguration->elegantConfiguration.accelerateJavaUDFs,
                             coordinatorConfiguration->elegantConfiguration.accelerationServiceURL));
}

bool QueryDeploymentPhase::execute(const SharedQueryPlanPtr& sharedQueryPlan) {
    NES_DEBUG("QueryDeploymentPhase: deploy the query");

    auto sharedQueryId = sharedQueryPlan->getId();

    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(sharedQueryId);
    if (executionNodes.empty()) {
        NES_ERROR("QueryDeploymentPhase: Unable to find ExecutionNodes to be deploy the shared query {}", sharedQueryId);
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
        queryCatalogService->updateQueryStatus(queryId, QueryStatus::DEPLOYED, "");
    }

    bool successDeploy = deployQuery(sharedQueryId, executionNodes);
    if (successDeploy) {
        NES_DEBUG("QueryDeploymentPhase: deployment for shared query {} successful", std::to_string(sharedQueryId));
    } else {
        NES_ERROR("QueryDeploymentPhase: Failed to deploy shared query {}", sharedQueryId);
        throw QueryDeploymentException(sharedQueryId,
                                       "QueryDeploymentPhase: Failed to deploy shared query " + std::to_string(sharedQueryId));
    }

    //Mark queries as running
    for (auto& queryId : sharedQueryPlan->getQueryIds()) {
        queryCatalogService->updateQueryStatus(queryId, QueryStatus::RUNNING, "");
    }

    NES_DEBUG("QueryService: start query");
    bool successStart = startQuery(sharedQueryId, executionNodes);
    if (successStart) {
        NES_DEBUG("QueryDeploymentPhase: Successfully started deployed shared query  {}", sharedQueryId);
    } else {
        NES_ERROR("QueryDeploymentPhase: Failed to start the deployed shared query {}", sharedQueryId);
        throw QueryDeploymentException(sharedQueryId,
                                       "QueryDeploymentPhase: Failed to deploy query " + std::to_string(sharedQueryId));
    }
    return true;
}

bool QueryDeploymentPhase::deployQuery(QueryId queryId, const std::vector<ExecutionNodePtr>& executionNodes) {
    NES_DEBUG("QueryDeploymentPhase::deployQuery queryId= {}", queryId);
    std::map<CompletionQueuePtr, uint64_t> completionQueues;
    for (const ExecutionNodePtr& executionNode : executionNodes) {
        NES_DEBUG("QueryDeploymentPhase::registerQueryInNodeEngine serialize id={}", executionNode->getId());
        std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
        if (querySubPlans.empty()) {
            NES_WARNING("QueryDeploymentPhase : unable to find query sub plan with id {}", queryId);
            return false;
        }

        CompletionQueuePtr queueForExecutionNode = std::make_shared<CompletionQueue>();

        const auto& nesNode = executionNode->getTopologyNode();
        auto ipAddress = nesNode->getIpAddress();
        auto grpcPort = nesNode->getGrpcPort();
        std::string rpcAddress = ipAddress + ":" + std::to_string(grpcPort);
        NES_DEBUG("QueryDeploymentPhase:deployQuery: {} to {}", queryId, rpcAddress);

        auto topologyNode = executionNode->getTopologyNode();

        for (auto& querySubPlan : querySubPlans) {

            //If accelerate Java UDFs is enabled
            if (accelerateJavaUDFs) {

                //Elegant acceleration service call
                //1. Fetch the OpenCL Operators
                auto openCLOperators = querySubPlan->getOperatorByType<OpenCLLogicalOperatorNode>();

                //2. Iterate over all open CL operators and set the Open CL code returned by the acceleration service
                for (const auto& openCLOperator : openCLOperators) {

                    // FIXME: populate information from node with the correct property keys
                    // FIXME: pick a naming + id scheme for deviceName
                    //3. Fetch the topology node and compute the topology node payload
                    nlohmann::json payload;
                    nlohmann::json deviceInfo;
                    deviceInfo[DEVICE_INFO_NAME_KEY] = std::any_cast<std::string>(topologyNode->getNodeProperty("DEVICE_NAME"));
                    deviceInfo[DEVICE_INFO_DOUBLE_FP_SUPPORT_KEY] = std::any_cast<bool>(topologyNode->getNodeProperty("DEVICE_DOUBLE_FP_SUPPORT"));
                    nlohmann::json maxWorkItems{};
                    maxWorkItems[DEVICE_MAX_WORK_ITEMS_DIM1_KEY] = std::any_cast<uint64_t>(topologyNode->getNodeProperty("DEVICE_MAX_WORK_ITEMS_DIM1"));
                    maxWorkItems[DEVICE_MAX_WORK_ITEMS_DIM2_KEY] = std::any_cast<uint64_t>(topologyNode->getNodeProperty("DEVICE_MAX_WORK_ITEMS_DIM2"));
                    maxWorkItems[DEVICE_MAX_WORK_ITEMS_DIM3_KEY] = std::any_cast<uint64_t>(topologyNode->getNodeProperty("DEVICE_MAX_WORK_ITEMS_DIM3"));
                    deviceInfo[DEVICE_MAX_WORK_ITEMS_KEY] = maxWorkItems;
                    deviceInfo[DEVICE_INFO_ADDRESS_BITS_KEY] = std::any_cast<std::string>(topologyNode->getNodeProperty("DEVICE_ADDRESS_BITS"));
                    deviceInfo[DEVICE_INFO_EXTENSIONS_KEY] = std::any_cast<std::string>(topologyNode->getNodeProperty("DEVICE_EXTENSIONS"));
                    deviceInfo[DEVICE_INFO_AVAILABLE_PROCESSORS_KEY] = std::any_cast<uint64_t>(topologyNode->getNodeProperty("DEVICE_AVAILABLE_PROCESSORS"));
                    payload[DEVICE_INFO_KEY] = deviceInfo;

                    //4. Extract the Java UDF code
                    auto javaDescriptor = openCLOperator->getJavaUDFDescriptor();
                    //FIXME: Add the UDF code
                    payload["functionCode"] = javaDescriptor->getMethodName();

                    //5. Make Acceleration Service Call
                    cpr::Response response = cpr::Post(cpr::Url{accelerationServiceURL},
                                                       cpr::Header{{"Content-Type", "application/json"}},
                                                       cpr::Body{payload.dump()},
                                                       cpr::Timeout(3000));
                    if (response.status_code != 200) {
                        throw QueryDeploymentException(
                            queryId,
                            "QueryDeploymentPhase::deployQuery: Error in call to Elegant acceleration service with code "
                                + std::to_string(response.status_code) + " and msg " + response.reason);
                    }

                    nlohmann::json jsonResponse = nlohmann::json::parse(response.text);
                    //Fetch the acceleration Code
                    //FIXME: use the correct key
                    auto openCLCode = jsonResponse["AccelerationCode"];
                    //6. Set the Open CL code
                    openCLOperator->setOpenClCode(openCLCode);
                }
            }

            //enable this for sync calls
            //bool success = workerRPCClient->registerQuery(rpcAddress, querySubPlan);
            bool success = workerRPCClient->registerQueryAsync(rpcAddress, querySubPlan, queueForExecutionNode);
            if (success) {
                NES_DEBUG("QueryDeploymentPhase:deployQuery: {} to {} successful", queryId, rpcAddress);
            } else {
                NES_ERROR("QueryDeploymentPhase:deployQuery: {} to {} failed", queryId, rpcAddress);
                return false;
            }
        }
        completionQueues[queueForExecutionNode] = querySubPlans.size();
    }
    bool result = workerRPCClient->checkAsyncResult(completionQueues, RpcClientModes::Register);
    NES_DEBUG("QueryDeploymentPhase: Finished deploying execution plan for query with Id {} success={}", queryId, result);
    return result;
}

bool QueryDeploymentPhase::startQuery(QueryId queryId, const std::vector<ExecutionNodePtr>& executionNodes) {
    NES_DEBUG("QueryDeploymentPhase::startQuery queryId= {}", queryId);
    //TODO: check if one queue can be used among multiple connections
    std::map<CompletionQueuePtr, uint64_t> completionQueues;

    for (const ExecutionNodePtr& executionNode : executionNodes) {
        CompletionQueuePtr queueForExecutionNode = std::make_shared<CompletionQueue>();

        const auto& nesNode = executionNode->getTopologyNode();
        auto ipAddress = nesNode->getIpAddress();
        auto grpcPort = nesNode->getGrpcPort();
        std::string rpcAddress = ipAddress + ":" + std::to_string(grpcPort);
        NES_DEBUG("QueryDeploymentPhase::startQuery at execution node with id={} and IP={}", executionNode->getId(), ipAddress);
        //enable this for sync calls
        //bool success = workerRPCClient->startQuery(rpcAddress, queryId);
        bool success = workerRPCClient->startQueryAsyn(rpcAddress, queryId, queueForExecutionNode);
        if (success) {
            NES_DEBUG("QueryDeploymentPhase::startQuery {} to {} successful", queryId, rpcAddress);
        } else {
            NES_ERROR("QueryDeploymentPhase::startQuery {} to {} failed", queryId, rpcAddress);
            return false;
        }
        completionQueues[queueForExecutionNode] = 1;
    }

    bool result = workerRPCClient->checkAsyncResult(completionQueues, RpcClientModes::Start);
    NES_DEBUG("QueryDeploymentPhase: Finished starting execution plan for query with Id {} success={}", queryId, result);
    return result;
}

}// namespace NES
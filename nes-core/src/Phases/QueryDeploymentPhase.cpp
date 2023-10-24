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

#include <Catalogs/Query/QueryCatalogService.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Exceptions/ExecutionNodeNotFoundException.hpp>
#include <Exceptions/QueryDeploymentException.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <Operators/LogicalOperators/OpenCLLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/UDFs/JavaUDFDescriptor.hpp>
#include <Phases/QueryDeploymentPhase.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger/Logger.hpp>
#include <cpr/cpr.h>
#include <nlohmann/json.hpp>
#include <utility>
#include <Configurations/WorkerConfigurationKeys.hpp>
#include <Runtime/OpenCLManager.hpp>

namespace NES {

QueryDeploymentPhase::QueryDeploymentPhase(const GlobalExecutionPlanPtr& globalExecutionPlan,
                                           const QueryCatalogServicePtr& catalogService,
                                           bool accelerateJavaUDFs,
                                           const std::string& accelerationServiceURL)
    : workerRPCClient(WorkerRPCClient::create()), globalExecutionPlan(globalExecutionPlan), queryCatalogService(catalogService),
      accelerateJavaUDFs(accelerateJavaUDFs), accelerationServiceURL(accelerationServiceURL) {}

QueryDeploymentPhasePtr
QueryDeploymentPhase::create(const GlobalExecutionPlanPtr& globalExecutionPlan,
                             const QueryCatalogServicePtr& catalogService,
                             const Configurations::CoordinatorConfigurationPtr& coordinatorConfiguration) {
    return std::make_shared<QueryDeploymentPhase>(
        QueryDeploymentPhase(globalExecutionPlan,
                             catalogService,
                             coordinatorConfiguration->elegant.accelerateJavaUDFs,
                             coordinatorConfiguration->elegant.accelerationServiceURL));
}

void QueryDeploymentPhase::execute(const SharedQueryPlanPtr& sharedQueryPlan) {
    NES_DEBUG("QueryDeploymentPhase: deploy the query");

    auto sharedQueryId = sharedQueryPlan->getId();

    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(sharedQueryId);
    if (executionNodes.empty()) {
        NES_ERROR("QueryDeploymentPhase: Unable to find ExecutionNodes where the query {} is deployed", sharedQueryId);
        throw Exceptions::ExecutionNodeNotFoundException("Unable to find ExecutionNodes where the query "
                                                         + std::to_string(sharedQueryId) + " is deployed");
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
        queryCatalogService->updateQueryStatus(queryId, QueryState::DEPLOYED, "");
    }

    deployQuery(sharedQueryId, executionNodes);
    NES_DEBUG("QueryDeploymentPhase: deployment for shared query {} successful", std::to_string(sharedQueryId));

    //Mark queries as running
    for (auto& queryId : sharedQueryPlan->getQueryIds()) {
        queryCatalogService->updateQueryStatus(queryId, QueryState::RUNNING, "");
    }

    NES_DEBUG("QueryService: start query");
    startQuery(sharedQueryId, executionNodes);
}

void QueryDeploymentPhase::deployQuery(QueryId queryId, const std::vector<ExecutionNodePtr>& executionNodes) {
    NES_DEBUG("QueryDeploymentPhase::deployQuery queryId= {}", queryId);
    std::map<CompletionQueuePtr, uint64_t> completionQueues;
    for (const ExecutionNodePtr& executionNode : executionNodes) {
        NES_DEBUG("QueryDeploymentPhase::registerQueryInNodeEngine serialize id={}", executionNode->getId());
        std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
        if (querySubPlans.empty()) {
            throw QueryDeploymentException(queryId,
                                           "QueryDeploymentPhase : unable to find query sub plan with id "
                                               + std::to_string(queryId));
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
                    payload[DEVICE_INFO_KEY] = std::any_cast<std::vector<NES::Runtime::OpenCLDeviceInfo>>(
                        topologyNode->getNodeProperty(NES::Worker::Configuration::OPENCL_DEVICES))[openCLOperator->getDeviceId()];

                    //4. Extract the Java UDF metadata
                    auto javaDescriptor = openCLOperator->getJavaUDFDescriptor();
                    payload["functionCode"] = javaDescriptor->getMethodName();

                    //find the bytecode for the udf class
                    auto className = javaDescriptor->getClassName();
                    auto byteCodeList = javaDescriptor->getByteCodeList();
                    auto classByteCode =
                        std::find_if(byteCodeList.cbegin(), byteCodeList.cend(), [&](const jni::JavaClassDefinition& c) {
                            return c.first == className;
                        });

                    if (classByteCode == byteCodeList.end()) {
                        throw QueryDeploymentException(queryId,
                                                       "The bytecode list of classes implementing the "
                                                       "UDF must contain the fully-qualified name of the UDF");
                    }
                    jni::JavaByteCode javaByteCode = classByteCode->second;

                    //5. Prepare the multi-part message
                    cpr::Part part1 = {"jsonFile", to_string(payload)};
                    cpr::Part part2 = {"codeFile", &javaByteCode[0]};
                    cpr::Multipart multipartPayload = cpr::Multipart{part1, part2};

                    //6. Make Acceleration Service Call
                    cpr::Response response = cpr::Post(cpr::Url{accelerationServiceURL},
                                                       cpr::Header{{"Content-Type", "application/json"}},
                                                       multipartPayload,
                                                       cpr::Timeout(ELEGANT_SERVICE_TIMEOUT));
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
            workerRPCClient->registerQueryAsync(rpcAddress, querySubPlan, queueForExecutionNode);
        }
        completionQueues[queueForExecutionNode] = querySubPlans.size();
    }
    workerRPCClient->checkAsyncResult(completionQueues, RpcClientModes::Register);
    NES_DEBUG("QueryDeploymentPhase: Finished deploying execution plan for query with Id {} ", queryId);
}

void QueryDeploymentPhase::startQuery(QueryId queryId, const std::vector<ExecutionNodePtr>& executionNodes) {
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
        workerRPCClient->startQueryAsync(rpcAddress, queryId, queueForExecutionNode);
        completionQueues[queueForExecutionNode] = 1;
    }

    workerRPCClient->checkAsyncResult(completionQueues, RpcClientModes::Start);
    NES_DEBUG("QueryDeploymentPhase: Finished starting execution plan for query with Id {}", queryId);
}

}// namespace NES
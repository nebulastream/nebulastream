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

#include <Catalogs/Query/QueryCatalogEntry.hpp>
#include <Catalogs/Query/QueryCatalogService.hpp>
#include <Catalogs/Query/QuerySubPlanMetaData.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/WorkerConfigurationKeys.hpp>
#include <Exceptions/ExecutionNodeNotFoundException.hpp>
#include <Exceptions/QueryDeploymentException.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <Operators/LogicalOperators/OpenCLLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/UDFs/JavaUDFDescriptor.hpp>
#include <Phases/QueryDeploymentPhase.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Runtime/OpenCLManager.hpp>
#include <Util/Logger/Logger.hpp>
#include <cpr/cpr.h>
#include <nlohmann/json.hpp>
#include <utility>

namespace NES {

QueryDeploymentPhase::QueryDeploymentPhase(const Optimizer::GlobalExecutionPlanPtr& globalExecutionPlan,
                                           const QueryCatalogServicePtr& catalogService,
                                           bool accelerateJavaUDFs,
                                           const std::string& accelerationServiceURL)
    : workerRPCClient(WorkerRPCClient::create()), globalExecutionPlan(globalExecutionPlan), queryCatalogService(catalogService),
      accelerateJavaUDFs(accelerateJavaUDFs), accelerationServiceURL(accelerationServiceURL) {}

QueryDeploymentPhasePtr
QueryDeploymentPhase::create(const Optimizer::GlobalExecutionPlanPtr& globalExecutionPlan,
                             const QueryCatalogServicePtr& catalogService,
                             const Configurations::CoordinatorConfigurationPtr& coordinatorConfiguration) {
    return std::make_shared<QueryDeploymentPhase>(QueryDeploymentPhase(globalExecutionPlan,
                                                                       catalogService,
                                                                       coordinatorConfiguration->elegant.accelerateJavaUDFs,
                                                                       coordinatorConfiguration->elegant.accelerationServiceURL));
}

void QueryDeploymentPhase::execute(const SharedQueryPlanPtr& sharedQueryPlan) {
    NES_DEBUG("QueryDeploymentPhase: deploy the query");

    auto sharedQueryId = sharedQueryPlan->getId();

    std::vector<Optimizer::ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(sharedQueryId);
    if (executionNodes.empty()) {
        NES_ERROR("QueryDeploymentPhase: Unable to find ExecutionNodes where the query {} is deployed", sharedQueryId);
        throw Exceptions::ExecutionNodeNotFoundException("Unable to find ExecutionNodes where the query "
                                                         + std::to_string(sharedQueryId) + " is deployed");
    }

    //Remove the old mapping of the shared query plan
    if (SharedQueryPlanStatus::MIGRATING != sharedQueryPlan->getStatus()) {
        if (SharedQueryPlanStatus::UPDATED == sharedQueryPlan->getStatus()) {
            queryCatalogService->removeSharedQueryPlanMapping(sharedQueryId);
        }

        //Reset all sub query plan metadata in the catalog
        for (const auto& queryId : sharedQueryPlan->getQueryIds()) {
            queryCatalogService->resetSubQueryMetaData(queryId);
            queryCatalogService->mapSharedQueryPlanId(sharedQueryId, queryId);
        }
    }

    //Add sub query plan metadata in the catalog
    for (auto& executionNode : executionNodes) {
        const auto workerId = executionNode->getId();
        const auto subQueryPlans = executionNode->getAllDecomposedQueryPlans(sharedQueryId);
        for (const auto& subQueryPlan : subQueryPlans) {
            QueryId querySubPlanId = subQueryPlan->getDecomposedQueryPlanId();
            NES_INFO("Updating state of decomposed query plan {} with old state {}", subQueryPlan->getDecomposedQueryPlanId(), magic_enum::enum_name(subQueryPlan->getState()))
            switch (subQueryPlan->getState()) {
                case QueryState::MARKED_FOR_DEPLOYMENT: subQueryPlan->setState(QueryState::DEPLOYED); break;
                case QueryState::MARKED_FOR_REDEPLOYMENT: subQueryPlan->setState(QueryState::REDEPLOYED); break;
                case QueryState::MARKED_FOR_MIGRATION: subQueryPlan->setState(QueryState::MIGRATING); break;
                case QueryState::RUNNING: break;            //do not modify anything for running plans
                case QueryState::MIGRATION_COMPLETED: break;//do not modfify plans that have been stopped after migration
                default: NES_FATAL_ERROR("Unexpected query plan state: {}", magic_enum::enum_name(subQueryPlan->getState()));
            }
            //todo #4452: avoid looping over all query ids by changing the structure of the query catalog
            for (auto& queryId : sharedQueryPlan->getQueryIds()) {
                //set metadata only for versions that deploy a new plan, reconfig and undeployment already have their metadata set
                auto entry = queryCatalogService->getEntryForQuery(queryId);
                if (!entry->hasQuerySubPlanMetaData(querySubPlanId)) {
                    queryCatalogService->addSubQueryMetaData(queryId, querySubPlanId, workerId, subQueryPlan->getState());
                } else {
                    entry->getQuerySubPlanMetaData(querySubPlanId)->updateStatus(subQueryPlan->getState());
                }
            }
        }
    }

    //Mark queries as deployed
    for (const auto& queryId : sharedQueryPlan->getQueryIds()) {
        //do not set migrating queries to deployed status
        if (sharedQueryPlan->getStatus() == SharedQueryPlanStatus::MIGRATING) {
            queryCatalogService->getEntryForQuery(queryId)->setQueryStatus(QueryState::MIGRATING);
        } else {
            queryCatalogService->getEntryForQuery(queryId)->setQueryStatus(QueryState::DEPLOYED);
        }
    }

    deployQuery(sharedQueryId, executionNodes);
    NES_DEBUG("QueryDeploymentPhase: deployment for shared query {} successful", std::to_string(sharedQueryId));

    //Mark queries as running if they are not in migrating state
    for (const auto& queryId : sharedQueryPlan->getQueryIds()) {
        if (sharedQueryPlan->getStatus() != SharedQueryPlanStatus::MIGRATING) {
            queryCatalogService->getEntryForQuery(queryId)->setQueryStatus(QueryState::RUNNING);
        }
    }

    //mark subqueries that were reconfigured as running again
    for (const auto& queryId : sharedQueryPlan->getQueryIds()) {
        for (const auto& subPlanMetaData : queryCatalogService->getEntryForQuery(queryId)->getAllSubQueryPlanMetaData()) {
            if (subPlanMetaData->getSubQueryStatus() == QueryState::REDEPLOYED) {
                subPlanMetaData->updateStatus(QueryState::RUNNING);
            }
        }
    }
    for (auto& executionNode : executionNodes) {
        const auto workerId = executionNode->getId();
        const auto subQueryPlans = executionNode->getAllDecomposedQueryPlans(sharedQueryId);
        for (auto& subQueryPlan : subQueryPlans) {
            auto querySubPlanId = subQueryPlan->getDecomposedQueryPlanId();
            if (subQueryPlan->getState() == QueryState::REDEPLOYED) {
                subQueryPlan->setState(QueryState::RUNNING);
            }
        }
    }

    NES_DEBUG("QueryDeploymentPhase: start query");
    startQuery(sharedQueryId, executionNodes);

    //remove subplans from global query plan if they were stopped due to migration
    for (const auto& node : executionNodes) {
        auto allDecomposedQueryPlans = node->getAllDecomposedQueryPlans(sharedQueryId);
        for (const auto& decomposedQueryPlan : allDecomposedQueryPlans) {
            auto subPlanStatus = decomposedQueryPlan->getState();
            if (subPlanStatus == QueryState::MIGRATING || subPlanStatus == QueryState::MIGRATION_COMPLETED) {
                NES_INFO("Removing migrated subplan with id  {} from execution node {}", decomposedQueryPlan->getDecomposedQueryPlanId(), node->getId())
//                globalExecutionPlan->removeQuerySubPlanFromNode(node->getId(),
//                                                                sharedQueryId,
//                                                                decomposedQueryPlan->getDecomposedQueryPlanId());
                node->removeDecomposedQueryPlan(sharedQueryId,
                                                                decomposedQueryPlan->getDecomposedQueryPlanId());
                auto resourceAmount = Optimizer::ExecutionNode::getOccupiedResourcesForDecomposedQueryPlan(decomposedQueryPlan);
                node->getTopologyNode()->releaseSlots(resourceAmount);
            }
        }
    }
}

void QueryDeploymentPhase::deployQuery(SharedQueryId sharedQueryId,
                                       const std::vector<Optimizer::ExecutionNodePtr>& executionNodes) {
    NES_DEBUG("QueryDeploymentPhase::deployQuery queryId= {}", sharedQueryId);
    std::map<CompletionQueuePtr, uint64_t> completionQueues;
    for (const Optimizer::ExecutionNodePtr& executionNode : executionNodes) {
        NES_DEBUG("QueryDeploymentPhase::registerExecutableQueryPlan serialize id={}", executionNode->getId());
        auto allDecomposedQueryPlans = executionNode->getAllDecomposedQueryPlans(sharedQueryId);
        if (allDecomposedQueryPlans.empty()) {
            throw QueryDeploymentException(sharedQueryId,
                                           "QueryDeploymentPhase : unable to find query sub plan with id "
                                               + std::to_string(sharedQueryId));
        }

        CompletionQueuePtr queueForExecutionNode = std::make_shared<CompletionQueue>();

        const auto& nesNode = executionNode->getTopologyNode();
        auto ipAddress = nesNode->getIpAddress();
        auto grpcPort = nesNode->getGrpcPort();
        std::string rpcAddress = ipAddress + ":" + std::to_string(grpcPort);
        NES_DEBUG("QueryDeploymentPhase:deployQuery: {} to {}", sharedQueryId, rpcAddress);

        completionQueues[queueForExecutionNode] = 0;
        for (const auto& decomposedQueryPlan : allDecomposedQueryPlans) {
            auto singleQueryId = queryCatalogService->getQueryIdsForSharedQueryId(sharedQueryId).front();
            auto subplanMetaData = queryCatalogService->getEntryForQuery(singleQueryId)
                                       ->getQuerySubPlanMetaData(decomposedQueryPlan->getDecomposedQueryPlanId());

            switch (decomposedQueryPlan->getState()) {
                case QueryState::DEPLOYED: {
                    //If accelerate Java UDFs is enabled
                    if (accelerateJavaUDFs) {
                        applyJavaUDFAcceleration(sharedQueryId, executionNode, decomposedQueryPlan);
                    }
                    //enable this for sync calls
                    //bool success = workerRPCClient->registerQuery(rpcAddress, querySubPlan);
                    workerRPCClient->registerQueryAsync(rpcAddress, decomposedQueryPlan, queueForExecutionNode);
                    decomposedQueryPlan->setState(QueryState::RUNNING);
                    subplanMetaData->updateStatus(decomposedQueryPlan->getState());
                    completionQueues[queueForExecutionNode]++;
                    break;
                }
                case QueryState::REDEPLOYED: {
                    //workerRPCClient->registerQuery(rpcAddress, decomposedQueryPlan);
                    workerRPCClient->registerQueryAsync(rpcAddress, decomposedQueryPlan, queueForExecutionNode);
                    completionQueues[queueForExecutionNode]++;
                    break;
                }
                default: {
                    break;
                }
            }
        }
    }
    workerRPCClient->checkAsyncResult(completionQueues, RpcClientModes::Register);
    NES_DEBUG("QueryDeploymentPhase: Finished deploying execution plan for query with Id {} ", sharedQueryId);
}

void QueryDeploymentPhase::applyJavaUDFAcceleration(
    SharedQueryId sharedQueryId,
    const Optimizer::ExecutionNodePtr& executionNode,
    const DecomposedQueryPlanPtr& decomposedQueryPlan) const {//Elegant acceleration service call
    //1. Fetch the OpenCL Operators
    auto openCLOperators = decomposedQueryPlan->getOperatorByType<OpenCLLogicalOperatorNode>();

    //2. Iterate over all open CL operators and set the Open CL code returned by the acceleration service
    for (const auto& openCLOperator : openCLOperators) {
        //3. Fetch the topology node and compute the topology node payload
        auto topologyNode = executionNode->getTopologyNode();
        nlohmann::json payload;
        payload[DEVICE_INFO_KEY] = std::any_cast<std::vector<Runtime::OpenCLDeviceInfo>>(
            topologyNode->getNodeProperty(Worker::Configuration::OPENCL_DEVICES))[openCLOperator->getDeviceId()];

        //4. Extract the Java UDF metadata
        auto javaDescriptor = openCLOperator->getJavaUDFDescriptor();
        payload["functionCode"] = javaDescriptor->getMethodName();

        // The constructor of the Java UDF descriptor ensures that the byte code of the class exists.
        jni::JavaByteCode javaByteCode = javaDescriptor->getClassByteCode(javaDescriptor->getClassName()).value();

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
            throw QueryDeploymentException(sharedQueryId,
                                           "Error in call to Elegant acceleration service with code "
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

void QueryDeploymentPhase::startQuery(QueryId queryId, const std::vector<Optimizer::ExecutionNodePtr>& executionNodes) {
    NES_DEBUG("QueryDeploymentPhase::startQuery queryId= {}", queryId);
    //TODO: check if one queue can be used among multiple connections
    std::map<CompletionQueuePtr, uint64_t> completionQueues;

    for (const Optimizer::ExecutionNodePtr& executionNode : executionNodes) {
        CompletionQueuePtr queueForExecutionNode = std::make_shared<CompletionQueue>();
        const auto& nesNode = executionNode->getTopologyNode();
        auto ipAddress = nesNode->getIpAddress();
        auto grpcPort = nesNode->getGrpcPort();
        std::string rpcAddress = ipAddress + ":" + std::to_string(grpcPort);
        NES_DEBUG("QueryDeploymentPhase::startQuery at execution node with id={} and IP={}", executionNode->getId(), ipAddress);

        std::vector<DecomposedQueryPlanId> migratingPlanIds;
        for (const auto& subPlan : executionNode->getAllDecomposedQueryPlans(queryId)) {
            if (subPlan->getState() == QueryState::MIGRATING) {
                migratingPlanIds.push_back(subPlan->getDecomposedQueryPlanId());
            }
        }
        //todo: make async call also for migrating
        if (!migratingPlanIds.empty()) {
            workerRPCClient->migrateSubplans(rpcAddress, migratingPlanIds);
        }

        //enable this for sync calls
        //bool success = workerRPCClient->startQuery(rpcAddress, queryId);
        workerRPCClient->startQueryAsync(rpcAddress, queryId, queueForExecutionNode);
        completionQueues[queueForExecutionNode] = 1;
    }
    workerRPCClient->checkAsyncResult(completionQueues, RpcClientModes::Start);
    NES_DEBUG("QueryDeploymentPhase: Finished starting execution plan for query with Id {}", queryId);
}

}// namespace NES
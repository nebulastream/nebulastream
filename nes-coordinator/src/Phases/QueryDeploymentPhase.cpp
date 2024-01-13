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
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Runtime/OpenCLManager.hpp>
#include <Util/Logger/Logger.hpp>
#include <cpr/cpr.h>
#include <nlohmann/json.hpp>
#include <utility>
#include <cpp-base64/base64.h>
#include <Elegant/ElegantAccelerationServiceClient.hpp>
#include <Exceptions/ElegantServiceException.hpp>

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
    return std::make_shared<QueryDeploymentPhase>(QueryDeploymentPhase(globalExecutionPlan,
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
        const auto subQueryPlans = executionNode->getQuerySubPlans(sharedQueryId);
        for (auto& subQueryPlan : subQueryPlans) {
            QueryId querySubPlanId = subQueryPlan->getQuerySubPlanId();
            //todo #4452: avoid looping over all query ids by changing the structure of the query catalog
            for (auto& queryId : sharedQueryPlan->getQueryIds()) {
                //set metadata only for versions that deploy a new plan, reconfig and undeployment already have their metadata set
                auto entry = queryCatalogService->getEntryForQuery(queryId);
                if (!entry->hasQuerySubPlanMetaData(querySubPlanId)) {
                    queryCatalogService->addSubQueryMetaData(queryId, querySubPlanId, workerId, QueryState::DEPLOYED);
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
            if (subPlanMetaData->getSubQueryStatus() == QueryState::RECONFIGURING) {
                subPlanMetaData->updateStatus(QueryState::RUNNING);
            }
        }
    }

    //remove subplans from global query plan if they were stopped due to migration
    auto singleQueryId = queryCatalogService->getQueryIdsForSharedQueryId(sharedQueryId).front();
    for (const auto& node : executionNodes) {
        auto subPlans = node->getQuerySubPlans(sharedQueryId);
        for (const auto& querySubPlan : subPlans) {
            const auto subplanMetaData =
                queryCatalogService->getEntryForQuery(singleQueryId)->getQuerySubPlanMetaData(querySubPlan->getQuerySubPlanId());
            auto subPlanStatus = subplanMetaData->getSubQueryStatus();
            if (subPlanStatus == QueryState::MIGRATING) {
                globalExecutionPlan->removeQuerySubPlanFromNode(node->getId(), sharedQueryId, querySubPlan->getQuerySubPlanId());
                auto resourceAmount = ExecutionNode::getOccupiedResourcesForSubPlan(querySubPlan);
                node->getTopologyNode()->releaseSlots(resourceAmount);
            }
        }
    }

    NES_DEBUG("QueryService: start query");
    startQuery(sharedQueryId, executionNodes);
}

void QueryDeploymentPhase::deployQuery(SharedQueryId sharedQueryId, const std::vector<ExecutionNodePtr>& executionNodes) {
    NES_DEBUG("QueryDeploymentPhase::deployQuery queryId= {}", sharedQueryId);
    std::map<CompletionQueuePtr, uint64_t> completionQueues;
    for (const ExecutionNodePtr& executionNode : executionNodes) {
        NES_DEBUG("QueryDeploymentPhase::registerQueryInNodeEngine serialize id={}", executionNode->getId());
        std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(sharedQueryId);
        if (querySubPlans.empty()) {
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

        for (const auto& querySubPlan : querySubPlans) {
            auto singleQueryId = queryCatalogService->getQueryIdsForSharedQueryId(sharedQueryId).front();
            auto subplanMetaData =
                queryCatalogService->getEntryForQuery(singleQueryId)->getQuerySubPlanMetaData(querySubPlan->getQuerySubPlanId());

            const auto subPlanState = subplanMetaData->getSubQueryStatus();
            switch (subPlanState) {
                case QueryState::DEPLOYED: {
                    //If accelerate Java UDFs is enabled
                    if (accelerateJavaUDFs) {
                        applyJavaUDFAcceleration(sharedQueryId, executionNode, querySubPlan);
                    }

                    //enable this for sync calls
                    //bool success = workerRPCClient->registerQuery(rpcAddress, querySubPlan);
                    workerRPCClient->registerQueryAsync(rpcAddress, querySubPlan, queueForExecutionNode);
                    subplanMetaData->updateStatus(QueryState::RUNNING);

                    completionQueues[queueForExecutionNode] = querySubPlans.size();
                    break;
                }
                case QueryState::RECONFIGURING: {
                    //todo #4440: make non async function for this
                    workerRPCClient->reconfigureQuery(rpcAddress, querySubPlan);
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

void QueryDeploymentPhase::applyJavaUDFAcceleration(SharedQueryId sharedQueryId,
                                                    [[maybe_unused]] const ExecutionNodePtr& executionNode,
                                                    const QueryPlanPtr& querySubPlan) const {
    auto openCLOperators = querySubPlan->getOperatorByType<OpenCLLogicalOperatorNode>();
    for (const auto& openCLOperator : openCLOperators) {

        // if (false) {
        //     // load kernel from file system
        //     const auto fileName = std::filesystem::path("../nes-coordinator/tests/TestData/computeNesMap.cl");
        //     if (!exists(fileName)) {
        //         NES_FATAL_ERROR("Could not find OpenCL file: {}", fileName.string());
        //     }
        //     std::ifstream openCLFile(fileName, std::fstream::in);
        //     openCLFile.seekg(0, std::ios_base::end);
        //     auto fileSize = openCLFile.tellg();
        //     openCLFile.seekg(0, std::ios_base::beg);
        //     std::vector<char> fileContents = std::vector<char>(fileSize);
        //     openCLFile.read(fileContents.data(), fileContents.size());
        //     openCLOperator->setOpenClCode(std::string(fileContents.data(), fileContents.size()));
        //     continue;
        // }

        try {
            ELEGANT::ElegantAccelerationServiceClient client{accelerationServiceURL};
            auto openCLCode = client.retrieveOpenCLKernel();
            openCLOperator->setOpenClCode(openCLCode);
            auto openCLDevice = std::any_cast<int32_t>(executionNode->getTopologyNode()->getNodeProperty(Worker::Configuration::DEFAULT_OPENCL_DEVICE));
            if (openCLDevice != -1) {
                NES_DEBUG("Using default OpenCL Device: {}", openCLDevice);
                openCLOperator->setDeviceId(openCLDevice);
            } else {
                NES_DEBUG("Using OpenCL device specified by Planner");
            }
        } catch (ELEGANT::ElegantServiceException e) {
            throw new QueryDeploymentException(sharedQueryId, e.what());
        }
    }
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
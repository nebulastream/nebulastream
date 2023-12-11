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
    //todo: we use updated here also for migration?
    if (SharedQueryPlanStatus::MIGRATING != sharedQueryPlan->getStatus()) {
        if (SharedQueryPlanStatus::Updated == sharedQueryPlan->getStatus()) {
            queryCatalogService->removeSharedQueryPlanMapping(sharedQueryId);
        }

        //todo: if we remove all metadata here we also need to add it again for all, but that could be optimized
        //Reset all sub query plan metadata in the catalog
        for (auto& queryId : sharedQueryPlan->getQueryIds()) {
            queryCatalogService->resetSubQueryMetaData(queryId);
            queryCatalogService->mapSharedQueryPlanId(sharedQueryId, queryId);
        }
    }

        //Add sub query plan metadata in the catalog
        for (auto& executionNode : executionNodes) {
            auto workerId = executionNode->getId();
            auto subQueryPlans = executionNode->getQuerySubPlans(sharedQueryId);
            for (auto& subQueryPlan : subQueryPlans) {
                QueryId querySubPlanId = subQueryPlan->getQuerySubPlanId();
                for (auto& queryId : sharedQueryPlan->getQueryIds()) {
                    //todo: do this only for versions that deploy a new plan, reconfig and undeployment already have their metadata set
                    auto entry = queryCatalogService->getEntryForQuery(queryId);
                    if (!entry->hasQuerySubPlanMetaData(querySubPlanId)) {
                        //we have to make sure, that the old execution node will not get a new plan here
                        queryCatalogService->addSubQueryMetaData(queryId, querySubPlanId, workerId, QueryState::DEPLOYED);
                    // } else if (entry->getQuerySubPlanMetaData(querySubPlanId)->getSubQueryStatus() == QueryState::SOFT_STOP_COMPLETED) {
                    //     entry->removeQuerySubPlanMetaData(querySubPlanId);
                    }
                }
            }
        }

    //Mark queries as deployed
    for (auto& queryId : sharedQueryPlan->getQueryIds()) {
        //todo: mark subqueries that will be undeployed and reconfigured as migrating here
        //todo: actually do we have to mark those that will be undeployed?
        //todo: just mark all as migrating?

        //todo: only overwrite if not migrating
        //if the query is migrating we can not yet put it to deployed
        //todo: meybe we only need to do that for the shared query plan
        auto newQueryState = sharedQueryPlan->getStatus() == SharedQueryPlanStatus::MIGRATING ? QueryState::MIGRATING : QueryState::DEPLOYED;
        //only update for the entry, not for the subquery metadata
        queryCatalogService->getEntryForQuery(queryId)->setQueryStatus(newQueryState);
        // if (queryCatalogService->getEntryForQuery(queryId)->getQueryState() != QueryState::MIGRATING) {
            //queryCatalogService->updateQueryStatus(queryId, QueryState::DEPLOYED, "");
        // }
    }

    deployQuery(sharedQueryId, executionNodes);
    NES_DEBUG("QueryDeploymentPhase: deployment for shared query {} successful", std::to_string(sharedQueryId));

    //todo: set only for sqp
    //Mark queries as running
    for (auto& queryId : sharedQueryPlan->getQueryIds()) {
        //todo: do not mark the queries to be undeployed as migrating
        //todo: move the condition outside the loop
        //auto newQueryState = sharedQueryPlan->getStatus() == SharedQueryPlanStatus::MIGRATING ? QueryState::MIGRATING : QueryState::RUNNING;
        if (sharedQueryPlan->getStatus() != SharedQueryPlanStatus::MIGRATING) {
            queryCatalogService->getEntryForQuery(queryId)->setQueryStatus(QueryState::RUNNING);
        }
//        if (queryCatalogService->getEntryForQuery(queryId)->getQueryState() != QueryState::MIGRATING) {
//            NES_DEBUG("Not updating query status because it is migrating");
//            queryCatalogService->updateQueryStatus(queryId, QueryState::RUNNING, "");
//        }
    }

    NES_DEBUG("QueryService: start query");
    startQuery(sharedQueryId, executionNodes);
}

//todo: query id is actually shared query id
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
            //todo qeuery state needs to be set
            //switch (querySubPlan->getQueryState()) {
            //switch (queryCatalogService->getEntryForQuery(queryId)->getQuerySubPlanMetaData(querySubPlan->getQuerySubPlanId())->getSubQueryStatus()) {
            auto subplanMetaData =
                queryCatalogService->getEntryForQuery(queryId)->getQuerySubPlanMetaData(querySubPlan->getQuerySubPlanId());
            auto subPlanState = subplanMetaData->getSubQueryStatus();
            switch (subPlanState) {
                //case QueryState::REGISTERED: {
                case QueryState::DEPLOYED: {
                    //If accelerate Java UDFs is enabled
                    if (accelerateJavaUDFs) {

                        //Elegant acceleration service call
                        //1. Fetch the OpenCL Operators
                        auto openCLOperators = querySubPlan->getOperatorByType<OpenCLLogicalOperatorNode>();

                        //2. Iterate over all open CL operators and set the Open CL code returned by the acceleration service
                        for (const auto& openCLOperator : openCLOperators) {

                            //3. Fetch the topology node and compute the topology node payload
                            nlohmann::json payload;
                            payload[DEVICE_INFO_KEY] =
                                std::any_cast<std::vector<NES::Runtime::OpenCLDeviceInfo>>(topologyNode->getNodeProperty(
                                    NES::Worker::Configuration::OPENCL_DEVICES))[openCLOperator->getDeviceId()];

                            //4. Extract the Java UDF metadata
                            auto javaDescriptor = openCLOperator->getJavaUDFDescriptor();
                            payload["functionCode"] = javaDescriptor->getMethodName();

                            // The constructor of the Java UDF descriptor ensures that the byte code of the class exists.
                            jni::JavaByteCode javaByteCode =
                                javaDescriptor->getClassByteCode(javaDescriptor->getClassName()).value();

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
                                throw QueryDeploymentException(queryId,
                                                               "Error in call to Elegant acceleration service with code "
                                                                   + std::to_string(response.status_code) + " and msg "
                                                                   + response.reason);
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
                    //todo: should this be deployed first?
                    //querySubPlan->setQueryState(QueryState::RUNNING);
                    subplanMetaData->updateStatus(QueryState::RUNNING);

                    completionQueues[queueForExecutionNode] = querySubPlans.size();
                    break;
                }
                case QueryState::RECONFIGURE: {
                    //todo: deploy reconfig
                    NES_NOT_IMPLEMENTED();
                    break;
                }
                case QueryState::SOFT_STOP_COMPLETED:
                case QueryState::MIGRATING: {
                    //todo: remove plan from execution node
                    //executionNode->removeQuerySubPlan(queryId, querySubPlan->getQuerySubPlanId());
                    //todo: increase resources
                    //todo: do we count subplans or operators?
                    //todo: freeing currently causes errror about consumed resources
                    //executionNode->getTopologyNode()->increaseResources(1);
                    //todo: if the metadate is already soft stopped, remove that too, otherwise mark MIGRATION_COMPLETED it so it will be removed at soft stop
                    //todo: we cannot do this because it might cause problems with concurrent operations
                    //todo: no its actually safe because the entry has a mutex
                    //todo: do not remove metadata here, just garbage callect on next deployment
                    // if (subplanMetaData->getSubQueryStatus() == QueryState::SOFT_STOP_COMPLETED) {
                    //     //todo: get the entry just once above and reuse here
                    //     queryCatalogService->getEntryForQuery(queryId)->removeQuerySubPlanMetaData(querySubPlan->getQuerySubPlanId());
                    // } else {
                    //     //mark the node to be removed
                    //     subplanMetaData->updateStatus(QueryState::MIGRATION_COMPLETED);
                    // }
                    break;
                }
                default: {
                    break;
                }
            }
        }
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
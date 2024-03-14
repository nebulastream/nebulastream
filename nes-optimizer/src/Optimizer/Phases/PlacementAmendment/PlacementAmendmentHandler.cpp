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

#include <Catalogs/Query/QueryCatalog.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Optimizer/Phases/PlacementAmendment/PlacementAmendmentHandler.hpp>
#include <Optimizer/Phases/PlacementAmendment/QueryPlacementAmendmentPhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Util/DeploymentContext.hpp>

namespace NES::Optimizer {

PlacementAmendmentHandler::PlacementAmendmentHandler(uint16_t numOfHandler, UMPMCAmendmentQueuePtr amendmentQueue)
    : running(true), numOfHandler(numOfHandler), amendmentQueue(amendmentQueue) {}

void PlacementAmendmentHandler::start() {
    // Initiate amendment runners
    NES_INFO("Initializing placement amendment handler {}", numOfHandler);
    for (size_t i = 0; i < numOfHandler; ++i) {
        amendmentRunners.emplace_back(std::thread([this]() {
            handleRequest();
        }));
    }
}

void PlacementAmendmentHandler::handleRequest() {
    while (running) {
        PlacementAmemderInstancePtr placementAmemderInstance;
        if (!amendmentQueue->try_dequeue(placementAmemderInstance)) {
            continue;
        }
        placementAmemderInstance->execute();
    }
}

void PlacementAmendmentHandler::shutDown() {
    running = false;
    //Join all runners and wait them to be completed before returning the call
    for (auto& amendmentRunner : amendmentRunners) {
        if (amendmentRunner.joinable()) {
            amendmentRunner.join();
        }
    }
}

PlacementAmemderInstancePtr PlacementAmemderInstance::create(SharedQueryPlanPtr sharedQueryPlan,
                                                             Optimizer::GlobalExecutionPlanPtr globalExecutionPlan,
                                                             TopologyPtr topology,
                                                             Optimizer::TypeInferencePhasePtr typeInferencePhase,
                                                             Configurations::CoordinatorConfigurationPtr coordinatorConfiguration,
                                                             Catalogs::Query::QueryCatalogPtr queryCatalog) {
    return std::make_unique<PlacementAmemderInstance>(sharedQueryPlan,
                                                      globalExecutionPlan,
                                                      topology,
                                                      typeInferencePhase,
                                                      coordinatorConfiguration,
                                                      queryCatalog);
}

PlacementAmemderInstance::PlacementAmemderInstance(SharedQueryPlanPtr sharedQueryPlan,
                                                   GlobalExecutionPlanPtr globalExecutionPlan,
                                                   TopologyPtr topology,
                                                   TypeInferencePhasePtr typeInferencePhase,
                                                   Configurations::CoordinatorConfigurationPtr coordinatorConfiguration,
                                                   Catalogs::Query::QueryCatalogPtr queryCatalog)
    : sharedQueryPlan(sharedQueryPlan), globalExecutionPlan(globalExecutionPlan), topology(topology),
      typeInferencePhase(typeInferencePhase), coordinatorConfiguration(coordinatorConfiguration), queryCatalog(queryCatalog){};

void PlacementAmemderInstance::execute() {
    auto queryPlacementAmendmentPhase = Optimizer::QueryPlacementAmendmentPhase::create(globalExecutionPlan,
                                                                                        topology,
                                                                                        typeInferencePhase,
                                                                                        coordinatorConfiguration);

    auto sharedQueryId = sharedQueryPlan->getId();
    auto deploymentContexts = queryPlacementAmendmentPhase->execute(sharedQueryPlan);

    // Iterate over deployment context and update execution plan
    for (const auto& deploymentContext : deploymentContexts) {
        auto executionNodeId = deploymentContext->getWorkerId();
        auto decomposedQueryPlanId = deploymentContext->getDecomposedQueryPlanId();
        auto decomposedQueryPlanVersion = deploymentContext->getDecomposedQueryPlanVersion();
        auto decomposedQueryPlanState = deploymentContext->getDecomposedQueryPlanState();
        switch (decomposedQueryPlanState) {
            case QueryState::MARKED_FOR_REDEPLOYMENT:
            case QueryState::MARKED_FOR_DEPLOYMENT: {
                globalExecutionPlan->updateDecomposedQueryPlanState(executionNodeId,
                                                                    sharedQueryId,
                                                                    decomposedQueryPlanId,
                                                                    decomposedQueryPlanVersion,
                                                                    QueryState::RUNNING);
                break;
            }
            case QueryState::MARKED_FOR_MIGRATION: {
                globalExecutionPlan->updateDecomposedQueryPlanState(executionNodeId,
                                                                    sharedQueryId,
                                                                    decomposedQueryPlanId,
                                                                    decomposedQueryPlanVersion,
                                                                    QueryState::STOPPED);
                globalExecutionPlan->removeDecomposedQueryPlan(executionNodeId,
                                                               sharedQueryId,
                                                               decomposedQueryPlanId,
                                                               decomposedQueryPlanVersion);
                break;
            }
            default: NES_WARNING("Unhandled Deployment context with status: {}", magic_enum::enum_name(decomposedQueryPlanState));
        }
    }
    if (sharedQueryPlan->getStatus() == SharedQueryPlanStatus::PROCESSED) {
        sharedQueryPlan->setStatus(SharedQueryPlanStatus::DEPLOYED);
        queryCatalog->updateSharedQueryStatus(sharedQueryId, QueryState::RUNNING, "");
    } else if (sharedQueryPlan->getStatus() == SharedQueryPlanStatus::PARTIALLY_PROCESSED) {
        sharedQueryPlan->setStatus(SharedQueryPlanStatus::UPDATED);
    } else if (sharedQueryPlan->getStatus() == SharedQueryPlanStatus::STOPPED) {
        queryCatalog->updateSharedQueryStatus(sharedQueryId, QueryState::STOPPED, "");
    }
    //Mark as completed
    completionPromise.set_value(true);
}

std::future<bool> PlacementAmemderInstance::getFuture() { return completionPromise.get_future(); }
}// namespace NES::Optimizer

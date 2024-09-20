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
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Optimizer/Phases/QueryPlacementAmendmentPhase.hpp>
#include <Phases/DeploymentPhase.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Reconfiguration/Metadata/DrainQueryMetadata.hpp>
#include <Reconfiguration/Metadata/UpdateQueryMetadata.hpp>
#include <Reconfiguration/ReconfigurationMarker.hpp>
#include <Services/PlacementAmendment/PlacementAmendmentInstance.hpp>
#include <Util/DeploymentContext.hpp>
#include <Util/QueryState.hpp>
#include <Util/magicenum/magic_enum.hpp>

namespace NES::Optimizer {

PlacementAmendmentInstancePtr
PlacementAmendmentInstance::create(SharedQueryPlanPtr sharedQueryPlan,
                                   Optimizer::GlobalExecutionPlanPtr globalExecutionPlan,
                                   TopologyPtr topology,
                                   Optimizer::TypeInferencePhasePtr typeInferencePhase,
                                   Configurations::CoordinatorConfigurationPtr coordinatorConfiguration,
                                   DeploymentPhasePtr deploymentPhase) {
    return std::make_unique<PlacementAmendmentInstance>(sharedQueryPlan,
                                                        globalExecutionPlan,
                                                        topology,
                                                        typeInferencePhase,
                                                        coordinatorConfiguration,
                                                        deploymentPhase);
}

PlacementAmendmentInstance::PlacementAmendmentInstance(SharedQueryPlanPtr sharedQueryPlan,
                                                       GlobalExecutionPlanPtr globalExecutionPlan,
                                                       TopologyPtr topology,
                                                       TypeInferencePhasePtr typeInferencePhase,
                                                       Configurations::CoordinatorConfigurationPtr coordinatorConfiguration,
                                                       DeploymentPhasePtr deploymentPhase)
    : sharedQueryPlan(sharedQueryPlan), globalExecutionPlan(globalExecutionPlan), topology(topology),
      typeInferencePhase(typeInferencePhase), coordinatorConfiguration(coordinatorConfiguration),
      deploymentPhase(deploymentPhase){};

void PlacementAmendmentInstance::execute() {
    try {
        // 1. Check if the incremental placement is enabled
        auto incrementalPlacement = coordinatorConfiguration->optimizer.enableIncrementalPlacement.getValue();
        // 2. get the status of the shared query plan
        SharedQueryPlanStatus sharedQueryPlanStatus = sharedQueryPlan->getStatus();
        // 3. Compute the request type
        RequestType requestType;
        switch (sharedQueryPlanStatus) {
            case SharedQueryPlanStatus::STOPPED: {
                requestType = RequestType::StopQuery;
                break;
            }
            case SharedQueryPlanStatus::FAILED: {
                requestType = RequestType::FailQuery;
                break;
            }
            case SharedQueryPlanStatus::MIGRATING:
            case SharedQueryPlanStatus::CREATED:
            case SharedQueryPlanStatus::UPDATED: {
                // If system is configured to perform incremental placement then mark the request for AddQuery
                // else mark the request as restart to allow performing holistic deployment by first un-deployment and then re-deployment
                if (incrementalPlacement) {
                    requestType = RequestType::AddQuery;
                } else {
                    requestType = RequestType::RestartQuery;
                }
                break;
            }
            default: {
                //Mark as completed
                NES_ERROR("Shared query plan in unhandled status", magic_enum::enum_name(sharedQueryPlanStatus));
                completionPromise.set_value(false);
                return;
            }
        }
        NES_DEBUG("Processing placement amendment request with type {}", magic_enum::enum_name(requestType));
        // 4. Call the placement amendment phase to remove/add invalid placements
        auto queryPlacementAmendmentPhase = Optimizer::QueryPlacementAmendmentPhase::create(globalExecutionPlan,
                                                                                            topology,
                                                                                            typeInferencePhase,
                                                                                            coordinatorConfiguration);
        auto deploymentUnit = queryPlacementAmendmentPhase->execute(sharedQueryPlan);
        // 5. Call the deployment phase to dispatch the updated decomposed query plans for deployment, un-deployment, or migration
        if (deploymentUnit.containsDeploymentContext()) {
            if (incrementalPlacement) {
                // Compute reconfiguration marker based on deployment contexts
                auto reconfigurationMarker = computeReconfigurationMarker(deploymentUnit);
            }
            //Undeploy all removed or migrating deployment contexts
            deploymentPhase->execute(deploymentUnit.deploymentRemovalContexts, requestType);
            //Remove all queries marked for removal from shared query plan
            sharedQueryPlan->removeQueryMarkedForRemoval();
            //Deploy all newly placed deployment contexts
            deploymentPhase->execute(deploymentUnit.deploymentAdditionContexts, requestType);
            // 6. Update the global execution plan to reflect the updated state of the decomposed query plans
            NES_DEBUG("Update global execution plan to reflect state of decomposed query plans")
            auto sharedQueryId = sharedQueryPlan->getId();
            // Iterate over deployment context and update execution plan
            for (const auto& deploymentContext : deploymentUnit.getAllDeploymentContexts()) {
                auto workerId = deploymentContext->getWorkerId();
                auto decomposedQueryId = deploymentContext->getDecomposedQueryId();
                auto decomposedQueryPlanVersion = deploymentContext->getDecomposedQueryPlanVersion();
                auto decomposedQueryPlanState = deploymentContext->getDecomposedQueryPlanState();
                switch (decomposedQueryPlanState) {
                    case QueryState::MARKED_FOR_REDEPLOYMENT:
                    case QueryState::MARKED_FOR_DEPLOYMENT: {
                        globalExecutionPlan->updateDecomposedQueryPlanState(workerId,
                                                                            sharedQueryId,
                                                                            decomposedQueryId,
                                                                            decomposedQueryPlanVersion,
                                                                            QueryState::RUNNING);
                        break;
                    }
                    case QueryState::MARKED_FOR_MIGRATION: {
                        globalExecutionPlan->updateDecomposedQueryPlanState(workerId,
                                                                            sharedQueryId,
                                                                            decomposedQueryId,
                                                                            decomposedQueryPlanVersion,
                                                                            QueryState::STOPPED);
                        globalExecutionPlan->removeDecomposedQueryPlan(workerId,
                                                                       sharedQueryId,
                                                                       decomposedQueryId,
                                                                       decomposedQueryPlanVersion);
                        break;
                    }
                    default:
                        NES_WARNING("Unhandled Deployment context with status: {}",
                                    magic_enum::enum_name(decomposedQueryPlanState));
                }
            }
        }
        // 7. Update the shared query plan and the query catalog
        NES_DEBUG("Update shared query plan status")
        SharedQueryPlanStatus sharedQueryPlanStatusPostPlacement = sharedQueryPlan->getStatus();
        if (sharedQueryPlanStatusPostPlacement == SharedQueryPlanStatus::PROCESSED) {
            sharedQueryPlan->setStatus(SharedQueryPlanStatus::DEPLOYED);
        } else if (sharedQueryPlanStatusPostPlacement == SharedQueryPlanStatus::PARTIALLY_PROCESSED) {
            sharedQueryPlan->setStatus(SharedQueryPlanStatus::UPDATED);
        }
        //Mark as completed
        completionPromise.set_value(true);
    } catch (std::exception ex) {
        NES_ERROR("Failed to perform placement amendment for shared query {} due to {}", sharedQueryPlan->getId(), ex.what());
        completionPromise.set_value(false);
    }
}

ReconfigurationMarkerPtr PlacementAmendmentInstance::computeReconfigurationMarker(DeploymentUnit& deploymentUnit) {
    NES_DEBUG("Computing reconfiguration marker.")
    auto reconfigurationMarker = ReconfigurationMarker::create();

    // There should not be anything to be removed
    if (!deploymentUnit.deploymentRemovalContexts.empty()) {
        NES_ERROR("Deployment unit should not contain deployment removal contexts. Found {} unhandled contexts.",
                  deploymentUnit.deploymentRemovalContexts.size());
        throw Exceptions::RuntimeException(
            "Unable to compute Reconfiguration marker as unhandled deployment removal contexts found.");
    }

    //Add information about newly added decomposed plans
    for (const auto& deploymentAdditionContext : deploymentUnit.deploymentAdditionContexts) {
        QueryState queryState = deploymentAdditionContext->getDecomposedQueryPlanState();
        switch (queryState) {
            case QueryState::MARKED_FOR_REDEPLOYMENT: {
                const auto& workerId = deploymentAdditionContext->getWorkerId();
                const auto& sharedQueryId = deploymentAdditionContext->getSharedQueryId();
                const auto& decomposedQueryId = deploymentAdditionContext->getDecomposedQueryId();
                auto key = workerId.toString() + "-" + sharedQueryId.toString() + "-" + decomposedQueryId.toString();
                const auto& decomposedQueryPlanVersion = deploymentAdditionContext->getDecomposedQueryPlanVersion();
                auto reConfMetaData =
                    std::make_shared<UpdateQueryMetadata>(workerId, sharedQueryId, decomposedQueryId, decomposedQueryPlanVersion);
                auto markerEvent = ReconfigurationMarkerEvent::create(queryState, reConfMetaData);
                reconfigurationMarker->addReconfigurationEvent(key, markerEvent);
                break;
            }
            case QueryState::MARKED_FOR_MIGRATION: {
                const auto& workerId = deploymentAdditionContext->getWorkerId();
                const auto& sharedQueryId = deploymentAdditionContext->getSharedQueryId();
                const auto& decomposedQueryId = deploymentAdditionContext->getDecomposedQueryId();
                auto key = workerId.toString() + "-" + sharedQueryId.toString() + "-" + decomposedQueryId.toString();
                // Fetch already deployed decomposed query plan from the execution plan to count number of sources the plan to be
                // terminated has. This will allow us to compute the number of reconfiguration markers to be received before
                // terminating the decomposed query.
                auto deployedDecomposedQueryPlan =
                    globalExecutionPlan->getCopyOfDecomposedQueryPlan(workerId, sharedQueryId, decomposedQueryId);
                auto numOfSourceOperators = deployedDecomposedQueryPlan->getSourceOperators().size();
                auto reConfMetaData = std::make_shared<DrainQueryMetadata>(numOfSourceOperators);
                auto markerEvent = ReconfigurationMarkerEvent::create(queryState, reConfMetaData);
                reconfigurationMarker->addReconfigurationEvent(key, markerEvent);
                break;
            }
            default: NES_DEBUG("Skip recording decomposed query plan in state {}", magic_enum::enum_name(queryState));
        }
    }
    return reconfigurationMarker;
}

std::future<bool> PlacementAmendmentInstance::getFuture() { return completionPromise.get_future(); }

void PlacementAmendmentInstance::setPromise(bool promise) { completionPromise.set_value(promise); }

}// namespace NES::Optimizer

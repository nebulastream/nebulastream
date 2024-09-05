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
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
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
    // 1. Compute the request type
    RequestType requestType;
    SharedQueryPlanStatus sharedQueryPlanStatus = sharedQueryPlan->getStatus();
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
            if (coordinatorConfiguration->optimizer.enableIncrementalPlacement) {
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

    // 2. Call the placement amendment phase to remove/add invalid placements
    auto queryPlacementAmendmentPhase = Optimizer::QueryPlacementAmendmentPhase::create(globalExecutionPlan,
                                                                                        topology,
                                                                                        typeInferencePhase,
                                                                                        coordinatorConfiguration);
    auto deploymentUnit = queryPlacementAmendmentPhase->execute(sharedQueryPlan);

    // 3. Call the deployment phase to dispatch the updated decomposed query plans for deployment, un-deployment, or migration
    if (deploymentUnit.containsDeploymentContext()) {

        //Undeploy all removed or migrating deployment contexts
        deploymentPhase->execute(deploymentUnit.deploymentRemovalContexts, requestType);
        //Remove all queries marked for removal from shared query plan
        sharedQueryPlan->removeQueryMarkedForRemoval();

        //Deploy all newly placed deployment contexts
        deploymentPhase->execute(deploymentUnit.deploymentAdditionContexts, requestType);

        // 4. Update the global execution plan to reflect the updated state of the decomposed query plans
        NES_DEBUG("Update global execution plan to reflect state of decomposed query plans")
        auto sharedQueryId = sharedQueryPlan->getId();
        // Iterate over deployment context and update execution plan
        for (const auto& deploymentContext : deploymentUnit.getAllDeploymentContexts()) {
            auto workerId = deploymentContext->getWorkerId();
            auto decomposedQueryPlanId = deploymentContext->getDecomposedQueryPlanId();
            auto decomposedQueryPlanVersion = deploymentContext->getDecomposedQueryPlanVersion();
            auto decomposedQueryPlanState = deploymentContext->getDecomposedQueryPlanState();
            switch (decomposedQueryPlanState) {
                case QueryState::MARKED_FOR_REDEPLOYMENT:
                case QueryState::MARKED_FOR_DEPLOYMENT: {
                    globalExecutionPlan->updateDecomposedQueryPlanState(workerId,
                                                                        sharedQueryId,
                                                                        decomposedQueryPlanId,
                                                                        decomposedQueryPlanVersion,
                                                                        QueryState::RUNNING);
                    break;
                }
                case QueryState::MARKED_FOR_MIGRATION: {
                    globalExecutionPlan->updateDecomposedQueryPlanState(workerId,
                                                                        sharedQueryId,
                                                                        decomposedQueryPlanId,
                                                                        decomposedQueryPlanVersion,
                                                                        QueryState::STOPPED);
                    globalExecutionPlan->removeDecomposedQueryPlan(workerId,
                                                                   sharedQueryId,
                                                                   decomposedQueryPlanId,
                                                                   decomposedQueryPlanVersion);
                    break;
                }
                default:
                    NES_WARNING("Unhandled Deployment context with status: {}", magic_enum::enum_name(decomposedQueryPlanState));
            }
        }
    }

    // 5. Update the shared query plan and the query catalog
    NES_DEBUG("Update shared query plan status")
    SharedQueryPlanStatus sharedQueryPlanStatusPostPlacement = sharedQueryPlan->getStatus();
    if (sharedQueryPlanStatusPostPlacement == SharedQueryPlanStatus::PROCESSED) {
        sharedQueryPlan->setStatus(SharedQueryPlanStatus::DEPLOYED);
    } else if (sharedQueryPlanStatusPostPlacement == SharedQueryPlanStatus::PARTIALLY_PROCESSED) {
        sharedQueryPlan->setStatus(SharedQueryPlanStatus::UPDATED);
    }
    //Mark as completed
    completionPromise.set_value(true);
}

std::future<bool> PlacementAmendmentInstance::getFuture() { return completionPromise.get_future(); }

void PlacementAmendmentInstance::setPromise(bool promise) { completionPromise.set_value(promise); }

}// namespace NES::Optimizer

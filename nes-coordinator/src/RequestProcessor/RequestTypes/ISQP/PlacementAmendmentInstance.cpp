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

#include <../../../../../nes-catalogs/include/Catalogs/Query/QueryCatalog.hpp>
#include <../../../../../nes-common/include/Util/QueryState.hpp>
#include <../../../../../nes-common/include/Util/magicenum/magic_enum.hpp>
#include <../../../../../nes-configurations/include/Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <../../../../../nes-optimizer/include/Optimizer/Phases/PlacementAmendment/QueryPlacementAmendmentPhase.hpp>
#include <../../../../../nes-optimizer/include/Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <../../../../../nes-optimizer/include/Plans/Global/Query/SharedQueryPlan.hpp>
#include <../../../../../nes-optimizer/include/Util/DeploymentContext.hpp>
#include <Phases/DeploymentPhase.hpp>
#include <RequestProcessor/RequestTypes/ISQP/PlacementAmendmentInstance.hpp>

namespace NES::Optimizer {

PlacementAmendmentInstancePtr
PlacementAmendmentInstance::create(SharedQueryPlanPtr sharedQueryPlan,
                                   Optimizer::GlobalExecutionPlanPtr globalExecutionPlan,
                                   TopologyPtr topology,
                                   Optimizer::TypeInferencePhasePtr typeInferencePhase,
                                   Configurations::CoordinatorConfigurationPtr coordinatorConfiguration,
                                   Catalogs::Query::QueryCatalogPtr queryCatalog,
                                   bool deploy) {
    return std::make_unique<PlacementAmendmentInstance>(sharedQueryPlan,
                                                        globalExecutionPlan,
                                                        topology,
                                                        typeInferencePhase,
                                                        coordinatorConfiguration,
                                                        queryCatalog,
                                                        deploy);
}

PlacementAmendmentInstance::PlacementAmendmentInstance(SharedQueryPlanPtr sharedQueryPlan,
                                                       GlobalExecutionPlanPtr globalExecutionPlan,
                                                       TopologyPtr topology,
                                                       TypeInferencePhasePtr typeInferencePhase,
                                                       Configurations::CoordinatorConfigurationPtr coordinatorConfiguration,
                                                       Catalogs::Query::QueryCatalogPtr queryCatalog,
                                                       bool deploy)
    : sharedQueryPlan(sharedQueryPlan), globalExecutionPlan(globalExecutionPlan), topology(topology),
      typeInferencePhase(typeInferencePhase), coordinatorConfiguration(coordinatorConfiguration), queryCatalog(queryCatalog),
      deploy(deploy){};

void PlacementAmendmentInstance::execute() {
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
    if (deploy) {
        //deployment phase
        auto deploymentPhase = DeploymentPhase::create(queryCatalog);
        deploymentPhase->execute(deploymentContexts, RequestType::AddQuery);
    }
    //Mark as completed
    completionPromise.set_value(true);
}

std::future<bool> PlacementAmendmentInstance::getFuture() { return completionPromise.get_future(); }
}// namespace NES::Optimizer
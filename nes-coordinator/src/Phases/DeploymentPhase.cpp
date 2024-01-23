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
#include <GRPC/WorkerRPCClient.hpp>
#include <Phases/DeploymentPhase.hpp>
#include <Util/DeploymentContext.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES {

DeploymentPhasePtr DeploymentPhase::create(const NES::QueryCatalogServicePtr& queryCatalogService,
                                           const Configurations::CoordinatorConfigurationPtr& coordinatorConfiguration) {
    bool accelerateJavaUDFs = coordinatorConfiguration->elegant.accelerateJavaUDFs.getValue();
    std::string accelerationServiceURL = coordinatorConfiguration->elegant.accelerationServiceURL.getValue();
    return std::make_shared<DeploymentPhase>(queryCatalogService, accelerateJavaUDFs, accelerationServiceURL);
}

DeploymentPhase::DeploymentPhase(const QueryCatalogServicePtr& queryCatalogService,
                                 bool accelerateJavaUDFs,
                                 const std::string& accelerationServiceURL)
    : workerRPCClient(WorkerRPCClient::create()), queryCatalogService(queryCatalogService),
      accelerateJavaUDFs(accelerateJavaUDFs), accelerationServiceURL(accelerationServiceURL) {
    NES_INFO("DeploymentPhase()");
}

void DeploymentPhase::execute(const std::set<Optimizer::DeploymentContextPtr>& deploymentContexts) {

    NES_INFO("Deploy {} decomposed queries.", deploymentContexts.size());
    deployQuery(deploymentContexts);

    NES_INFO("Start {} decomposed queries.", deploymentContexts.size());
    startQuery(deploymentContexts);
}

void DeploymentPhase::deployQuery(const std::set<Optimizer::DeploymentContextPtr>& deploymentContexts) {

    for (const auto& deploymentContext : deploymentContexts) {
        auto grpcAddress = deploymentContext->getGrpcAddress();
        auto decomposedQueryPlan = deploymentContext->getDecomposedQueryPlan();
        auto decomposedQueryPlanState = deploymentContext->getDecomposedQueryPlanState();
        switch (decomposedQueryPlanState) {
            case QueryState::MARKED_FOR_DEPLOYMENT:
            case QueryState::MARKED_FOR_REDEPLOYMENT:
            case QueryState::MARKED_FOR_MIGRATION: {
                workerRPCClient->registerQueryAsync(grpcAddress, decomposedQueryPlan);
                break;
            }
            case QueryState::MARKED_FOR_FAILURE:
            case QueryState::MARKED_FOR_HARD_STOP:
            case QueryState::MARKED_FOR_SOFT_STOP: {
                workerRPCClient->unregisterQueryAsync(grpcAddress, deploymentContext->getSharedQueryId());
                break;
            }
            default: {
                NES_WARNING("Can not hande decomposed query plan in the state {}",
                            magic_enum::enum_name(decomposedQueryPlanState));
            }
        }
    }
}

void DeploymentPhase::startQuery(const std::set<Optimizer::DeploymentContextPtr>& deploymentContexts) {

    for (const auto& deploymentContext : deploymentContexts) {
        auto grpcAddress = deploymentContext->getGrpcAddress();
        auto sharedQueryId = deploymentContext->getSharedQueryId();
        workerRPCClient->startQuery(grpcAddress, sharedQueryId);
    }
}

}// namespace NES

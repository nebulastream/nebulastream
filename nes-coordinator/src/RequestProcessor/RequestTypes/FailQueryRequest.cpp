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

#include <Catalogs/Exceptions/InvalidQueryStateException.hpp>
#include <Catalogs/Query/QueryCatalog.hpp>
#include <Exceptions/QueryUndeploymentException.hpp>
#include <Exceptions/RuntimeException.hpp>
#include <Optimizer/Phases/PlacementAmendment/QueryPlacementAmendmentPhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Phases/DeploymentPhase.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <RequestProcessor/RequestTypes/FailQueryRequest.hpp>
#include <RequestProcessor/StorageHandles/ResourceType.hpp>
#include <RequestProcessor/StorageHandles/StorageHandler.hpp>
#include <Util/DeploymentContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/RequestType.hpp>
#include <Util/magicenum/magic_enum.hpp>

namespace NES::RequestProcessor
{

FailQueryRequest::FailQueryRequest(
    const SharedQueryId sharedQueryId,
    const DecomposedQueryPlanId failedDecomposedPlanId,
    const std::string& failureReason,
    const uint8_t maxRetries)
    : AbstractUniRequest(
        {ResourceType::GlobalQueryPlan,
         ResourceType::QueryCatalogService,
         ResourceType::Topology,
         ResourceType::UdfCatalog,
         ResourceType::SourceCatalog,
         ResourceType::GlobalExecutionPlan,
         ResourceType::CoordinatorConfiguration,
         ResourceType::StatisticProbeHandler},
        maxRetries)
    , sharedQueryId(sharedQueryId)
    , decomposedQueryPlanId(failedDecomposedPlanId)
    , failureReason(failureReason)
{
}

FailQueryRequestPtr FailQueryRequest::create(
    SharedQueryId sharedQueryId, DecomposedQueryPlanId failedDecomposedQueryId, const std::string& failureReason, uint8_t maxRetries)
{
    return std::make_shared<FailQueryRequest>(sharedQueryId, failedDecomposedQueryId, failureReason, maxRetries);
}

void FailQueryRequest::preRollbackHandle(std::exception_ptr, const StorageHandlerPtr&)
{
}

std::vector<AbstractRequestPtr> FailQueryRequest::rollBack(std::exception_ptr ex, const StorageHandlerPtr&)
{
    //make sure the promise is set before returning in case a the caller is waiting on it
    trySetExceptionInPromise(ex);
    return {};
}

void FailQueryRequest::postRollbackHandle(std::exception_ptr, const StorageHandlerPtr&)
{
    //todo #3727: perform error handling
}

void FailQueryRequest::postExecution(const StorageHandlerPtr& storageHandler)
{
    storageHandler->releaseResources(requestId);
}

std::vector<AbstractRequestPtr> FailQueryRequest::executeRequestLogic(const StorageHandlerPtr& storageHandle)
{
    globalQueryPlan = storageHandle->getGlobalQueryPlanHandle(requestId);
    globalExecutionPlan = storageHandle->getGlobalExecutionPlanHandle(requestId);
    queryCatalog = storageHandle->getQueryCatalogHandle(requestId);
    topology = storageHandle->getTopologyHandle(requestId);
    coordinatorConfiguration = storageHandle->getCoordinatorConfiguration(requestId);
    auto sourceCatalog = storageHandle->getSourceCatalogHandle(requestId);
    auto udfCatalog = storageHandle->getUDFCatalogHandle(requestId);
    typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);

    //todo 4255: allow requests to skip to the front of the line
    queryCatalog->checkAndMarkSharedQueryForFailure(sharedQueryId, decomposedQueryPlanId);

    globalQueryPlan->removeQuery(UNSURE_CONVERSION_TODO_4761(sharedQueryId, QueryId), RequestType::FailQuery);

    auto queryPlacementAmendmentPhase
        = Optimizer::QueryPlacementAmendmentPhase::create(globalExecutionPlan, topology, typeInferencePhase, coordinatorConfiguration);
    auto sharedQueryPlan = globalQueryPlan->getSharedQueryPlan(sharedQueryId);
    auto deploymentContexts = queryPlacementAmendmentPhase->execute(sharedQueryPlan);

    //undeploy queries
    try
    {
        auto deploymentPhase = DeploymentPhase::create(queryCatalog);
        deploymentPhase->execute(deploymentContexts, RequestType::FailQuery);
    }
    catch (NES::Exceptions::RuntimeException& e)
    {
        throw Exceptions::QueryUndeploymentException(
            sharedQueryId, fmt::format("Failed to undeploy shared query with id {}", sharedQueryId));
    }

    queryCatalog->updateSharedQueryStatus(sharedQueryId, QueryState::FAILED, failureReason);

    //respond to the calling service which is the shared query id to the query being undeployed
    responsePromise.set_value(std::make_shared<FailQueryResponse>(sharedQueryId));

    // Iterate over deployment context and update execution plan
    for (const auto& deploymentContext : deploymentContexts)
    {
        auto WorkerId = deploymentContext->getWorkerId();
        auto decomposedQueryPlanId = deploymentContext->getDecomposedQueryPlanId();
        auto decomposedQueryPlanVersion = deploymentContext->getDecomposedQueryPlanVersion();
        auto decomposedQueryPlanState = deploymentContext->getDecomposedQueryPlanState();
        switch (decomposedQueryPlanState)
        {
            case QueryState::MARKED_FOR_REDEPLOYMENT:
            case QueryState::MARKED_FOR_DEPLOYMENT: {
                globalExecutionPlan->updateDecomposedQueryPlanState(
                    WorkerId, sharedQueryId, decomposedQueryPlanId, decomposedQueryPlanVersion, QueryState::RUNNING);
                break;
            }
            case QueryState::MARKED_FOR_MIGRATION: {
                globalExecutionPlan->updateDecomposedQueryPlanState(
                    WorkerId, sharedQueryId, decomposedQueryPlanId, decomposedQueryPlanVersion, QueryState::STOPPED);
                globalExecutionPlan->removeDecomposedQueryPlan(WorkerId, sharedQueryId, decomposedQueryPlanId, decomposedQueryPlanVersion);
                break;
            }
            default:
                NES_WARNING("Unhandled Deployment context with status: {}", magic_enum::enum_name(decomposedQueryPlanState));
        }
    }
    //no follow up requests
    return {};
    //todo #3727: catch exceptions for error handling
}
} // namespace NES::RequestProcessor

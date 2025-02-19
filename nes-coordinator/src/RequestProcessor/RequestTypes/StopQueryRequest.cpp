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

#include <Catalogs/Exceptions/InvalidQueryException.hpp>
#include <Catalogs/Exceptions/InvalidQueryStateException.hpp>
#include <Catalogs/Exceptions/QueryNotFoundException.hpp>
#include <Catalogs/Query/QueryCatalog.hpp>
#include <Exceptions/ExecutionNodeNotFoundException.hpp>
#include <Exceptions/QueryDeploymentException.hpp>
#include <Exceptions/QueryUndeploymentException.hpp>
#include <Operators/Exceptions/TypeInferenceException.hpp>
#include <Optimizer/Exceptions/QueryPlacementAmendmentException.hpp>
#include <Optimizer/Phases/QueryPlacementAmendmentPhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Phases/DeploymentPhase.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <RequestProcessor/RequestTypes/FailQueryRequest.hpp>
#include <RequestProcessor/RequestTypes/StopQueryRequest.hpp>
#include <Services/PlacementAmendment/PlacementAmendmentHandler.hpp>
#include <Services/PlacementAmendment/PlacementAmendmentInstance.hpp>
#include <Util/DeploymentContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/RequestType.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <string>

namespace NES::RequestProcessor {

StopQueryRequest::StopQueryRequest(QueryId queryId,
                                   uint8_t maxRetries,
                                   const Optimizer::PlacementAmendmentHandlerPtr& placementAmendmentHandler)
    : AbstractUniRequest({ResourceType::QueryCatalogService,
                          ResourceType::GlobalExecutionPlan,
                          ResourceType::Topology,
                          ResourceType::GlobalQueryPlan,
                          ResourceType::UdfCatalog,
                          ResourceType::SourceCatalog,
                          ResourceType::CoordinatorConfiguration,
                          ResourceType::StatisticProbeHandler},
                         maxRetries),
      queryId(queryId), placementAmendmentHandler(placementAmendmentHandler) {}

StopQueryRequestPtr StopQueryRequest::create(QueryId queryId,
                                             uint8_t maxRetries,
                                             const Optimizer::PlacementAmendmentHandlerPtr& placementAmendmentHandler) {
    return std::make_shared<StopQueryRequest>(queryId, maxRetries, placementAmendmentHandler);
}

std::vector<AbstractRequestPtr> StopQueryRequest::executeRequestLogic(const StorageHandlerPtr& storageHandler) {
    NES_TRACE("Start Stop Request logic.");
    std::vector<AbstractRequestPtr> failureRequests = {};
    try {
        NES_TRACE("Acquire Resources.");
        globalExecutionPlan = storageHandler->getGlobalExecutionPlanHandle(requestId);
        topology = storageHandler->getTopologyHandle(requestId);
        queryCatalog = storageHandler->getQueryCatalogHandle(requestId);
        globalQueryPlan = storageHandler->getGlobalQueryPlanHandle(requestId);
        udfCatalog = storageHandler->getUDFCatalogHandle(requestId);
        sourceCatalog = storageHandler->getSourceCatalogHandle(requestId);
        coordinatorConfiguration = storageHandler->getCoordinatorConfiguration(requestId);
        NES_TRACE("Locks acquired. Create Phases");
        typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
        NES_TRACE("Phases created. Stop request initialized.");

        if (queryId == INVALID_QUERY_ID) {
            throw Exceptions::QueryNotFoundException(
                fmt::format("Cannot stop query with invalid query id {}. Please enter a valid query id.", queryId));
        }

        //1. Mark the query for hard stop
        queryCatalog->updateQueryStatus(queryId, QueryState::MARKED_FOR_HARD_STOP, "Query Stop Requested");

        //2. Check if a shared query exists for the given query id
        auto sharedQueryId = globalQueryPlan->getSharedQueryId(queryId);
        if (sharedQueryId == INVALID_SHARED_QUERY_ID) {
            throw Exceptions::QueryNotFoundException(
                fmt::format("Could not find a a valid shared query plan for query with id {} in the global query plan",
                            sharedQueryId));
        }

        // 3. remove the query from global query plan that will in turn update the shared query plan
        globalQueryPlan->removeQuery(queryId, RequestType::StopQuery);

        // 3. Extract the shared query plan
        auto sharedQueryPlan = globalQueryPlan->getSharedQueryPlan(sharedQueryId);
        if (!sharedQueryPlan) {
            throw Exceptions::QueryNotFoundException(
                fmt::format("Could not find a a valid shared query plan for query with id {} in the global query plan",
                            sharedQueryId));
        }

        //4. Perform placement amendment and deployment of updated shared query plan
        NES_DEBUG("Performing Operator placement amendment and deployment for shared query plan");
        auto deploymentPhase = DeploymentPhase::create(queryCatalog);
        const auto& amendmentInstance = Optimizer::PlacementAmendmentInstance::create(sharedQueryPlan,
                                                                                      globalExecutionPlan,
                                                                                      topology,
                                                                                      typeInferencePhase,
                                                                                      coordinatorConfiguration,
                                                                                      queryCatalog,
                                                                                      deploymentPhase);
        placementAmendmentHandler->enqueueRequest(amendmentInstance);

        //FIXME: #3742 This may not work well when shared query plan contains more than one queries:
        // 1. The query merging feature is enabled.
        // 2. A query from a shared query plan was removed but over all shared query plan is still serving other queries.
        // Expected Result:
        //  - only the stopped query is marked as stopped.
        // Actual Result:
        //  - All queries are set to stopped and the whole shared query plan is removed.
        bool success = amendmentInstance->getFuture().get().success;
        if (!success) {
            NES_ERROR("Unable to stop query {}", queryId);
        }

        //5. Mark query as stopped
        queryCatalog->updateQueryStatus(queryId, QueryState::STOPPED, "Hard Stopped");
        globalQueryPlan->removeSharedQueryPlan(sharedQueryId);
        responsePromise.set_value(std::make_shared<StopQueryResponse>(success));
    } catch (RequestExecutionException& e) {
        NES_ERROR("{}", e.what());
        auto requests = handleError(std::current_exception(), storageHandler);
        failureRequests.insert(failureRequests.end(), requests.begin(), requests.end());
    }
    return failureRequests;
}

void StopQueryRequest::postExecution([[maybe_unused]] const StorageHandlerPtr& storageHandler) {
    storageHandler->releaseResources(requestId);
}

std::string StopQueryRequest::toString() { return fmt::format("StopQueryRequest {{ QueryId: {}}}", queryId); }

void StopQueryRequest::preRollbackHandle([[maybe_unused]] std::exception_ptr ex,
                                         [[maybe_unused]] const StorageHandlerPtr& storageHandle) {}

void StopQueryRequest::postRollbackHandle([[maybe_unused]] std::exception_ptr ex,
                                          [[maybe_unused]] const StorageHandlerPtr& storageHandle) {}

std::vector<AbstractRequestPtr> StopQueryRequest::rollBack(std::exception_ptr exception,
                                                           [[maybe_unused]] const StorageHandlerPtr& storageHandler) {
    std::vector<AbstractRequestPtr> failRequest;

    try {
        std::rethrow_exception(exception);
    } catch (Exceptions::QueryPlacementAmendmentException& ex) {
        failRequest.push_back(FailQueryRequest::create(UNSURE_CONVERSION_TODO_4761(ex.getQueryId(), SharedQueryId),
                                                       INVALID_DECOMPOSED_QUERY_PLAN_ID,
                                                       INVALID_DECOMPOSED_QUERY_PLAN_VERSION,
                                                       ex.what(),
                                                       MAX_RETRIES_FOR_FAILURE,
                                                       placementAmendmentHandler));
    } catch (QueryDeploymentException& ex) {
        //todo: #3821 change to more specific exceptions, remove QueryDeploymentException
        //Happens if:
        //1. QueryDeploymentException The bytecode list of classes implementing the UDF must contain the fully-qualified name of the UDF
        //2. QueryDeploymentException: Error in call to Elegant acceleration service with code
        //3. QueryDeploymentException: QueryDeploymentPhase : unable to find query sub plan with id
        failRequest.push_back(FailQueryRequest::create(UNSURE_CONVERSION_TODO_4761(ex.getQueryId(), SharedQueryId),
                                                       INVALID_DECOMPOSED_QUERY_PLAN_ID,
                                                       INVALID_DECOMPOSED_QUERY_PLAN_VERSION,
                                                       ex.what(),
                                                       MAX_RETRIES_FOR_FAILURE,
                                                       placementAmendmentHandler));
    } catch (InvalidQueryException& ex) {
        //Happens if:
        //1. InvalidQueryException: inside QueryDeploymentPhase, if the query sub-plan metadata already exists in the query catalog --> non-recoverable
        failRequest.push_back(FailQueryRequest::create(UNSURE_CONVERSION_TODO_4761(ex.getQueryId(), SharedQueryId),
                                                       INVALID_DECOMPOSED_QUERY_PLAN_ID,
                                                       INVALID_DECOMPOSED_QUERY_PLAN_VERSION,
                                                       ex.what(),
                                                       MAX_RETRIES_FOR_FAILURE,
                                                       placementAmendmentHandler));
    } catch (TypeInferenceException& ex) {
        queryCatalog->updateQueryStatus(ex.getQueryId(), QueryState::FAILED, ex.what());
    } catch (Exceptions::QueryUndeploymentException& ex) {
        // In general, failures in QueryUndeploymentPhase are concerned with the current sqp id and a failure with a topology node
        // Therefore, for QueryUndeploymentException, we assume that the sqp is not running on any node, and we can set the sqp's status to stopped
        // we do this as long as there are retries present, otherwise, we fail the query
        queryCatalog->updateQueryStatus(ex.getQueryId(), QueryState::FAILED, ex.what());
    } catch (Exceptions::QueryNotFoundException& ex) {
        //Could not find sqp in global query plan
        trySetExceptionInPromise(exception);
    } catch (Exceptions::ExecutionNodeNotFoundException& ex) {
        //could not obtain execution nodes by shared query id --> non-recoverable
        //Could not find topology node to release resources
        //Could not remove query sub plan from execution node
        //--> SQP is not running on any nodes:
        trySetExceptionInPromise(exception);
    } catch (Exceptions::InvalidQueryStateException& ex) {
        //2. if check and mark for hard stop failed, means that stop is already in process, hence, we don't do anything
        trySetExceptionInPromise(exception);
    } catch (Exceptions::RequestExecutionException& ex) {
        //todo: #3821 retry for these errors, add specific rpcCallException and retry failed part, differentiate between deployment and undeployment phase
        //RPC call errors:
        //1. asynchronous call to worker to stop shared query plan failed --> currently invokes NES_THROW_RUNTIME_ERROR;
        //2. asynchronous call to worker to unregister shared query plan failed --> currently invokes NES_THROW_RUNTIME_ERROR:
        setExceptionInPromiseOrRethrow(exception);
    } catch (Exceptions::RuntimeException& ex) {
        //todo: #3821 change to more specific exceptions
        //1. Called from QueryUndeploymentPhase: GlobalQueryPlan: Unable to remove all child operators of the identified sink operator in the shared query plan
        //2. Called from PlacementStrategyPhase: PlacementStrategyFactory: Unknown placement strategy
        setExceptionInPromiseOrRethrow(exception);
    }

    //make sure the promise is set before returning in case a the caller is waiting on it
    trySetExceptionInPromise(
        std::make_exception_ptr<RequestExecutionException>(RequestExecutionException("No return value set in promise")));
    return failRequest;
}
}// namespace NES::RequestProcessor
// namespace NES

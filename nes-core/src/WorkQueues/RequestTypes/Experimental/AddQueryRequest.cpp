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
#include <Catalogs/Query/QueryCatalogEntry.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Exceptions/BlockingOperatorException.hpp>
#include <Exceptions/ExecutionNodeNotFoundException.hpp>
#include <Exceptions/GlobalQueryPlanUpdateException.hpp>
#include <Exceptions/InvalidLogicalOperatorNodeException.hpp>
#include <Exceptions/InvalidQueryStateException.hpp>
#include <Exceptions/MapEntryNotFoundException.hpp>
#include <Exceptions/PhysicalSourceNotFoundException.hpp>
#include <Exceptions/QueryDeploymentException.hpp>
#include <Exceptions/QueryNotFoundException.hpp>
#include <Exceptions/QueryPlacementException.hpp>
#include <Exceptions/SharedQueryPlanNotFoundException.hpp>
#include <Exceptions/SignatureComputationException.hpp>
#include <Exceptions/TypeInferenceException.hpp>
#include <Exceptions/UDFException.hpp>
#include <Operators/LogicalOperators/Sinks/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/NetworkSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/Phases/GlobalQueryPlanUpdatePhase.hpp>
#include <Optimizer/Phases/MemoryLayoutSelectionPhase.hpp>
#include <Optimizer/Phases/OriginIdInferencePhase.hpp>
#include <Optimizer/Phases/QueryMergerPhase.hpp>
#include <Optimizer/Phases/QueryPlacementPhase.hpp>
#include <Optimizer/Phases/QueryRewritePhase.hpp>
#include <Optimizer/Phases/SampleCodeGenerationPhase.hpp>
#include <Optimizer/Phases/SignatureInferencePhase.hpp>
#include <Optimizer/Phases/TopologySpecificQueryRewritePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Phases/QueryDeploymentPhase.hpp>
#include <Phases/QueryUndeploymentPhase.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Services/QueryCatalogService.hpp>
#include <Topology/Topology.hpp>
#include <Util/Logger/Logger.hpp>
#include <WorkQueues/RequestTypes/Experimental/AddQueryRequest.hpp>
#include <WorkQueues/StorageHandles/ResourceType.hpp>
#include <WorkQueues/StorageHandles/StorageHandler.hpp>
#include <string>
#include <utility>

namespace NES::Experimental {
AddQueryRequest::AddQueryRequest(const QueryPlanPtr& queryPlan,
                                 Optimizer::PlacementStrategy queryPlacementStrategy,
                                 uint8_t maxRetries,
                                 NES::WorkerRPCClientPtr workerRpcClient,
                                 Configurations::CoordinatorConfigurationPtr coordinatorConfiguration,
                                 z3::ContextPtr z3Context)
    : AbstractRequest({ResourceType::QueryCatalogService,
                       ResourceType::GlobalExecutionPlan,
                       ResourceType::Topology,
                       ResourceType::GlobalQueryPlan,
                       ResourceType::UdfCatalog,
                       ResourceType::SourceCatalog},
                      maxRetries),
      workerRpcClient(std::move(workerRpcClient)), queryId(queryPlan->getQueryId()),
      coordinatorConfiguration(std::move(coordinatorConfiguration)), queryPlan(queryPlan),
      queryPlacementStrategy(queryPlacementStrategy), z3Context(std::move(z3Context)) {}

std::shared_ptr<AddQueryRequest> AddQueryRequest::create(const QueryPlanPtr& queryPlan,
                                                         Optimizer::PlacementStrategy queryPlacementStrategy,
                                                         uint8_t maxRetries,
                                                         NES::WorkerRPCClientPtr workerRpcClient,
                                                         Configurations::CoordinatorConfigurationPtr coordinatorConfiguration,
                                                         z3::ContextPtr z3Context) {
    return std::make_shared<AddQueryRequest>(queryPlan,
                                             queryPlacementStrategy,
                                             maxRetries,
                                             std::move(workerRpcClient),
                                             coordinatorConfiguration,
                                             std::move(z3Context));
}

void AddQueryRequest::preRollbackHandle([[maybe_unused]] const RequestExecutionException& ex,
                                        [[maybe_unused]] StorageHandler& storageHandler) {}

std::vector<AbstractRequestPtr> AddQueryRequest::rollBack([[maybe_unused]] RequestExecutionException& ex,
                                                          [[maybe_unused]] StorageHandler& storageHandler) {
    try {
        NES_TRACE("Error: {}", ex.what());
        if (ex.instanceOf<Exceptions::QueryNotFoundException>()) {
            NES_ERROR("{}", ex.what());
        } else if (ex.instanceOf<Exceptions::InvalidQueryStateException>() || ex.instanceOf<MapEntryNotFoundException>()
                   || ex.instanceOf<TypeInferenceException>() || ex.instanceOf<SignatureComputationException>()
                   || ex.instanceOf<Exceptions::PhysicalSourceNotFoundException>()
                   || ex.instanceOf<Exceptions::SharedQueryPlanNotFoundException>() || ex.instanceOf<UDFException>()
                   || ex.instanceOf<Exceptions::BlockingOperatorException>()
                   || ex.instanceOf<Exceptions::InvalidLogicalOperatorNodeException>()
                   || ex.instanceOf<GlobalQueryPlanUpdateException>()) {
            NES_ERROR("{}", ex.what());
            queryCatalogService->updateQueryStatus(queryId, QueryState::FAILED, "Query failed due to " + std::string(ex.what()));
        } else if (ex.instanceOf<Exceptions::QueryPlacementException>()
                   || ex.instanceOf<Exceptions::ExecutionNodeNotFoundException>()
                   || ex.instanceOf<QueryDeploymentException>()) {
            NES_ERROR("{}", ex.what());
            globalQueryPlan->removeQuery(queryId, RequestType::FailQuery);
            queryCatalogService->updateQueryStatus(queryId, QueryState::FAILED, "Query failed due to " + std::string(ex.what()));
        } else {
            NES_ERROR("Unknown exception: {}", ex.what());
        }
    } catch (RequestExecutionException& e) {
        if (retry()) {
            handleError(e, storageHandler);
        } else {
            NES_ERROR("StopQueryRequest: Final failure to rollback. No retries left. Error: {}", e.what());
        }
    }
    return {};
}

void AddQueryRequest::postRollbackHandle([[maybe_unused]] const RequestExecutionException& ex,
                                         [[maybe_unused]] StorageHandler& storageHandler) {}

void AddQueryRequest::postExecution([[maybe_unused]] StorageHandler& storageHandler) {}

std::vector<AbstractRequestPtr> AddQueryRequest::executeRequestLogic(StorageHandler& storageHandler) {
    try {
        globalExecutionPlan = storageHandler.getGlobalExecutionPlanHandle(requestId);
        topology = storageHandler.getTopologyHandle(requestId);
        queryCatalogService = storageHandler.getQueryCatalogServiceHandle(requestId);
        globalQueryPlan = storageHandler.getGlobalQueryPlanHandle(requestId);
        udfCatalog = storageHandler.getUDFCatalogHandle(requestId);
        sourceCatalog = storageHandler.getSourceCatalogHandle(requestId);
        NES_DEBUG("Locks acquired. Start creating phases.");
        typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
        queryPlacementPhase =
            Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, coordinatorConfiguration);
        queryDeploymentPhase =
            QueryDeploymentPhase::create(globalExecutionPlan, workerRpcClient, queryCatalogService, coordinatorConfiguration);
        queryUndeploymentPhase = QueryUndeploymentPhase::create(topology, globalExecutionPlan, workerRpcClient);
        auto optimizerConfigurations = coordinatorConfiguration->optimizer;
        queryMergerPhase = Optimizer::QueryMergerPhase::create(this->z3Context, optimizerConfigurations.queryMergerRule);
        typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, std::move(udfCatalog));
        sampleCodeGenerationPhase = Optimizer::SampleCodeGenerationPhase::create();
        queryRewritePhase = Optimizer::QueryRewritePhase::create(coordinatorConfiguration);
        originIdInferencePhase = Optimizer::OriginIdInferencePhase::create();
        topologySpecificQueryRewritePhase =
            Optimizer::TopologySpecificQueryRewritePhase::create(this->topology, sourceCatalog, optimizerConfigurations);
        signatureInferencePhase =
            Optimizer::SignatureInferencePhase::create(this->z3Context, optimizerConfigurations.queryMergerRule);
        memoryLayoutSelectionPhase = Optimizer::MemoryLayoutSelectionPhase::create(optimizerConfigurations.memoryLayoutPolicy);
        NES_INFO("Phases created. Add request initialized.");

        //1. Add the initial version of the query to the query catalog
        queryCatalogService->addUpdatedQueryPlan(queryId, "Input Query Plan", queryPlan);
        NES_INFO("Request received for optimizing and deploying of the query {}", queryId);

        //2. Set query status as Optimizing
        queryCatalogService->updateQueryStatus(queryId, QueryState::OPTIMIZING, "");

        //3. Execute type inference phase
        NES_DEBUG("Performing Query type inference phase for query:  {}", queryId);
        queryPlan = typeInferencePhase->execute(queryPlan);

        //4. Set memory layout of each logical operator
        NES_DEBUG("Performing query choose memory layout phase:  {}", queryId);
        queryPlan = memoryLayoutSelectionPhase->execute(queryPlan);

        //5. Perform query re-write
        NES_DEBUG("Performing Query rewrite phase for query:  {}", queryId);
        queryPlan = queryRewritePhase->execute(queryPlan);

        //6. Add the updated query plan to the query catalog
        queryCatalogService->addUpdatedQueryPlan(queryId, "Query Rewrite Phase", queryPlan);

        //7. Execute type inference phase on rewritten query plan
        queryPlan = typeInferencePhase->execute(queryPlan);

        //8. Generate sample code for elegant planner
        if (queryPlacementStrategy == Optimizer::PlacementStrategy::ELEGANT_BALANCED
            || queryPlacementStrategy == Optimizer::PlacementStrategy::ELEGANT_PERFORMANCE
            || queryPlacementStrategy == Optimizer::PlacementStrategy::ELEGANT_ENERGY) {
            queryPlan = sampleCodeGenerationPhase->execute(queryPlan);
        }

        //9. Perform signature inference phase for sharing identification among query plans
        signatureInferencePhase->execute(queryPlan);

        //10. Perform topology specific rewrites to the query plan
        queryPlan = topologySpecificQueryRewritePhase->execute(queryPlan);

        //11. Add the updated query plan to the query catalog
        queryCatalogService->addUpdatedQueryPlan(queryId, "Topology Specific Query Rewrite Phase", queryPlan);

        //12. Perform type inference over re-written query plan
        queryPlan = typeInferencePhase->execute(queryPlan);

        //13. Identify the number of origins and their ids for all logical operators
        queryPlan = originIdInferencePhase->execute(queryPlan);

        //14. Set memory layout of each logical operator in the rewritten query
        NES_DEBUG("Performing query choose memory layout phase:  {}", queryId);
        queryPlan = memoryLayoutSelectionPhase->execute(queryPlan);

        //15. Add the updated query plan to the query catalog
        queryCatalogService->addUpdatedQueryPlan(queryId, "Executed Query Plan", queryPlan);

        //16. Add the updated query plan to the global query plan
        NES_DEBUG("Performing Query type inference phase for query:  {}", queryId);
        globalQueryPlan->addQueryPlan(queryPlan);

        //17. Perform query merging for newly added query plan
        NES_DEBUG("Applying Query Merger Rules as Query Merging is enabled.");
        queryMergerPhase->execute(globalQueryPlan);

        //18. Get the shared query plan id for the added query
        auto sharedQueryId = globalQueryPlan->getSharedQueryId(queryId);
        if (sharedQueryId == INVALID_SHARED_QUERY_ID) {
            throw Exceptions::SharedQueryPlanNotFoundException(
                "Could not find shared query id in global query plan. Shared query id is invalid.",
                sharedQueryId);
        }

        //19. Get the shared query plan for the added query
        auto sharedQueryPlan = globalQueryPlan->getSharedQueryPlan(sharedQueryId);
        if (!sharedQueryPlan) {
            throw Exceptions::SharedQueryPlanNotFoundException("Could not obtain shared query plan by shared query id.",
                                                               sharedQueryId);
        }

        //20. If the shared query plan is not new but updated an existing shared query plan, we first need to undeploy that plan
        if (SharedQueryPlanStatus::Updated == sharedQueryPlan->getStatus()) {

            NES_DEBUG("Shared Query Plan is non empty and an older version is already "
                      "running.");
            //20.1 First undeploy the running shared query plan with the shared query plan id
            queryUndeploymentPhase->execute(sharedQueryId, SharedQueryPlanStatus::Updated);
        }
        //21. Perform placement of updated shared query plan
        NES_DEBUG("Performing Operator placement for shared query plan");
        if (!queryPlacementPhase->execute(sharedQueryPlan)) {
            throw Exceptions::QueryPlacementException(sharedQueryId,
                                                      "QueryProcessingService: Failed to perform query placement for "
                                                      "query plan with shared query id: "
                                                          + std::to_string(sharedQueryId));
        }

        //22. Perform deployment of re-placed shared query plan
        queryDeploymentPhase->execute(sharedQueryPlan);

        //23. Update the shared query plan as deployed
        sharedQueryPlan->setStatus(SharedQueryPlanStatus::Deployed);
    } catch (RequestExecutionException& e) {
        handleError(e, storageHandler);
    }
    return {};
}

}// namespace NES::Experimental
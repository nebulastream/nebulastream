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
#include <Exceptions/QueryDeploymentException.hpp>
#include <Exceptions/QueryPlacementException.hpp>
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
#include <string>
#include <utility>

namespace NES::Experimental {
AddQueryRequest::AddQueryRequest(const RequestId requestId,
                                 const QueryPlanPtr& queryPlan,
                                 Optimizer::PlacementStrategy queryPlacementStrategy,
                                 uint8_t maxRetries,
                                 NES::WorkerRPCClientPtr workerRpcClient,
                                 Configurations::CoordinatorConfigurationPtr coordinatorConfiguration,
                                 z3::ContextPtr z3Context,
                                 std::promise<AddQueryResponse> responsePromise)
    : AbstractRequest(requestId,
                      {ResourceType::QueryCatalogService,
                       ResourceType::GlobalExecutionPlan,
                       ResourceType::Topology,
                       ResourceType::GlobalQueryPlan,
                       ResourceType::UdfCatalog,
                       ResourceType::SourceCatalog},
                      maxRetries,
                      std::move(responsePromise)),
      workerRpcClient(workerRpcClient), queryId(queryPlan->getQueryId()), coordinatorConfiguration(coordinatorConfiguration),
      queryPlan(queryPlan), queryPlacementStrategy(queryPlacementStrategy), z3Context(std::move(z3Context)) {}

std::shared_ptr<AddQueryRequest> AddQueryRequest::create(const RequestId requestId,
                                                         const QueryPlanPtr& queryPlan,
                                                         Optimizer::PlacementStrategy queryPlacementStrategy,
                                                         uint8_t maxRetries,
                                                         NES::WorkerRPCClientPtr workerRpcClient,
                                                         Configurations::CoordinatorConfigurationPtr coordinatorConfiguration,
                                                         z3::ContextPtr z3Context,
                                                         std::promise<AddQueryResponse> responsePromise) {
    return std::make_shared<AddQueryRequest>(requestId,
                                             queryPlan,
                                             queryPlacementStrategy,
                                             maxRetries,
                                             std::move(workerRpcClient),
                                             coordinatorConfiguration,
                                             std::move(z3Context),
                                             std::move(responsePromise));
}

void AddQueryRequest::preRollbackHandle([[maybe_unused]] const RequestExecutionException& ex,
                                        [[maybe_unused]] StorageHandler& storageHandler) {}

std::vector<AbstractRequestPtr> AddQueryRequest::rollBack([[maybe_unused]] RequestExecutionException& ex,
                               [[maybe_unused]] StorageHandler& storageHandle) {
    return {};
}

void AddQueryRequest::postRollbackHandle([[maybe_unused]] const RequestExecutionException& ex,
                                         [[maybe_unused]] StorageHandler& storageHandler) {

    //todo: #4038 add error handling
}

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

        //19. Get the shared query plan for the added query
        auto sharedQueryPlan = globalQueryPlan->getSharedQueryPlan(sharedQueryId);

        //20. If the shared query plan is newly created
        if (SharedQueryPlanStatus::Created == sharedQueryPlan->getStatus()) {

            NES_DEBUG("Shared Query Plan is new.");

            //20.1 Perform placement of new shared query plan
            NES_DEBUG("Performing Operator placement for shared query plan");
            bool placementSuccessful = queryPlacementPhase->execute(sharedQueryPlan);
            if (!placementSuccessful) {
                throw Exceptions::QueryPlacementException(sharedQueryId,
                                                          "Failed to perform query placement for "
                                                          "query plan with shared query id: "
                                                              + std::to_string(sharedQueryId));
            }

            //20.2 Perform deployment of placed shared query plan
            queryDeploymentPhase->execute(sharedQueryPlan);

            //Update the shared query plan as deployed
            sharedQueryPlan->setStatus(SharedQueryPlanStatus::Deployed);

            //20.3 Check if the shared query plan was updated after addition or removal of operators
        } else if (SharedQueryPlanStatus::Updated == sharedQueryPlan->getStatus()) {

            NES_DEBUG("Shared Query Plan is non empty and an older version is already "
                      "running.");

            //20.4 First undeploy the running shared query plan with the shared query plan id
            queryUndeploymentPhase->execute(sharedQueryId, SharedQueryPlanStatus::Updated);

            //20.5 Perform placement of updated shared query plan
            NES_DEBUG("Performing Operator placement for shared query plan");
            bool placementSuccessful = queryPlacementPhase->execute(sharedQueryPlan);
            if (!placementSuccessful) {
                throw Exceptions::QueryPlacementException(sharedQueryId,
                                                          "QueryProcessingService: Failed to perform query placement for "
                                                          "query plan with shared query id: "
                                                              + std::to_string(sharedQueryId));
            }

            //20.6 Perform deployment of re-placed shared query plan
            queryDeploymentPhase->execute(sharedQueryPlan);

            //Update the shared query plan as deployed
            sharedQueryPlan->setStatus(SharedQueryPlanStatus::Deployed);
        }
    } catch (RequestExecutionException& e) {
        handleError(e, storageHandler);
    }
    return {};
}

}// namespace NES::Experimental
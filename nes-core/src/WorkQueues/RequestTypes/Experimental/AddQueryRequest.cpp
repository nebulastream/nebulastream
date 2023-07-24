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
#include <Exceptions/QueryDeploymentException.hpp>
#include <Exceptions/QueryNotFoundException.hpp>
#include <Exceptions/QueryPlacementException.hpp>
#include <Optimizer/Phases/QueryPlacementPhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Phases/QueryDeploymentPhase.hpp>
#include <Phases/QueryUndeploymentPhase.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Services/QueryCatalogService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/RequestType.hpp>
#include <WorkQueues/RequestTypes/Experimental/AddQueryRequest.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <string>
#include <utility>

namespace NES {
namespace Experimental {

AddQueryRequest::AddQueryRequest(const QueryPlanPtr& queryPlan,
                                 Optimizer::PlacementStrategy queryPlacementStrategy,
                                 uint8_t maxRetries,
                                 NES::WorkerRPCClientPtr workerRpcClient)
    : AbstractRequest(
        {
            ResourceType::QueryCatalogService,
            ResourceType::GlobalExecutionPlan,
            ResourceType::Topology,
            ResourceType::GlobalQueryPlan,
            ResourceType::UdfCatalog,
            ResourceType::SourceCatalog,
        },
        maxRetries),
      workerRpcClient(workerRpcClient), queryId(queryPlan->getQueryId()), queryPlan(queryPlan), queryPlacementStrategy(queryPlacementStrategy) {}

void AddQueryRequest::preRollbackHandle(RequestExecutionException& ex, StorageHandler& storageHandler) {}

void AddQueryRequest::rollBack(RequestExecutionException& ex, StorageHandler& storageHandle) {}

void AddQueryRequest::postRollbackHandle(RequestExecutionException& ex, StorageHandler& storageHandler) {}

void AddQueryRequest::postExecution(StorageHandler& storageHandler) {}

void AddQueryRequest::executeRequestLogic(StorageHandler& storageHandler) {
    try {
        globalExecutionPlan = storageHandler.getGlobalExecutionPlanHandle();
        topology = storageHandler.getTopologyHandle();
        queryCatalogService = storageHandler.getQueryCatalogServiceHandle();
        globalQueryPlan = storageHandler.getGlobalQueryPlanHandle();
        udfCatalog = storageHandler.getUDFCatalogHandle();
        sourceCatalog = storageHandler.getSourceCatalogHandle();
        NES_TRACE("Locks acquired. Create Phases");
        typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
        queryPlacementPhase =
            Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, coordinatorConfiguration);
        queryDeploymentPhase =
            QueryDeploymentPhase::create(globalExecutionPlan, workerRpcClient, queryCatalogService, coordinatorConfiguration);
        queryUndeploymentPhase = QueryUndeploymentPhase::create(topology, globalExecutionPlan, workerRpcClient);
        NES_TRACE("Phases created. Add request initialized.");

        //1. Add the initial version of the query to the query catalog
        queryCatalogService->addUpdatedQueryPlan(queryId, "Input Query Plan", queryPlan);
        NES_INFO("QueryProcessingService: Request received for optimizing and deploying of the query {}", queryId);

        //2. Set query status as Optimizing
        queryCatalogService->updateQueryStatus(queryId, QueryStatus::OPTIMIZING, "");

        //3. Execute type inference phase
        NES_DEBUG("QueryProcessingService: Performing Query type inference phase for query:  {}", queryId);
        queryPlan = typeInferencePhase->execute(queryPlan);

        //4. Set memory layout of each logical operator
        NES_DEBUG("QueryProcessingService: Performing query choose memory layout phase:  {}", queryId);
        queryPlan = setMemoryLayoutPhase->execute(queryPlan);

        //5. Perform query re-write
        NES_DEBUG("QueryProcessingService: Performing Query rewrite phase for query:  {}", queryId);
        queryPlan = queryRewritePhase->execute(queryPlan);

        //6. Add the updated query plan to the query catalog
        queryCatalogService->addUpdatedQueryPlan(queryId, "Query Rewrite Phase", queryPlan);

        //7. Execute type inference phase on rewritten query plan
        queryPlan = typeInferencePhase->execute(queryPlan);

        //8. Generate sample code for elegant planner
        if (addQueryRequest->getQueryPlacementStrategy() == PlacementStrategy::ELEGANT_BALANCED
            || addQueryRequest->getQueryPlacementStrategy() == PlacementStrategy::ELEGANT_PERFORMANCE
            || addQueryRequest->getQueryPlacementStrategy() == PlacementStrategy::ELEGANT_ENERGY) {
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
        NES_DEBUG("QueryProcessingService: Performing query choose memory layout phase:  {}", queryId);
        queryPlan = setMemoryLayoutPhase->execute(queryPlan);

        //15. Add the updated query plan to the query catalog
        queryCatalogService->addUpdatedQueryPlan(queryId, "Executed Query Plan", queryPlan);

        //16. Add the updated query plan to the global query plan
        NES_DEBUG("QueryProcessingService: Performing Query type inference phase for query:  {}", queryId);
        globalQueryPlan->addQueryPlan(queryPlan);

        //17. Perform query merging for newly added query plan
        NES_DEBUG("QueryProcessingService: Applying Query Merger Rules as Query Merging is enabled.");
        queryMergerPhase->execute(globalQueryPlan);


    } catch (RequestExecutionException& e) {
        handleError(e, storageHandler);
    }
}

}// namespace Experimental
}// namespace NES
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
#include <Exceptions/ExecutionNodeNotFoundException.hpp>
#include <Exceptions/GlobalQueryPlanUpdateException.hpp>
#include <Exceptions/InvalidLogicalOperatorException.hpp>
#include <Exceptions/InvalidQueryStateException.hpp>
#include <Exceptions/LogicalSourceNotFoundException.hpp>
#include <Exceptions/MapEntryNotFoundException.hpp>
#include <Exceptions/OperatorNotFoundException.hpp>
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
#include <Optimizer/Phases/QueryDeploymentPhase.hpp>
#include <Optimizer/Phases/QueryMergerPhase.hpp>
#include <Optimizer/Phases/QueryPlacementPhase.hpp>
#include <Optimizer/Phases/QueryRewritePhase.hpp>
#include <Optimizer/Phases/QueryUndeploymentPhase.hpp>
#include <Optimizer/Phases/SampleCodeGenerationPhase.hpp>
#include <Optimizer/Phases/SignatureInferencePhase.hpp>
#include <Optimizer/Phases/TopologySpecificQueryRewritePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryValidation/SemanticQueryValidation.hpp>
#include <Optimizer/QueryValidation/SyntacticQueryValidation.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Utils/PlanIdGenerator.hpp>
#include <Services/QueryCatalogService.hpp>
#include <Services/QueryParsingService.hpp>
#include <Topology/Topology.hpp>
#include <Util/Logger/Logger.hpp>
#include <WorkQueues/RequestTypes/Experimental/AddQueryRequest.hpp>
#include <WorkQueues/StorageHandles/ResourceType.hpp>
#include <WorkQueues/StorageHandles/StorageHandler.hpp>
#include <string>
#include <utility>

namespace NES::Experimental {

AddQueryRequest::AddQueryRequest(const std::string& queryString,
                                 const Optimizer::PlacementStrategy queryPlacementStrategy,
                                 const FaultToleranceType faultTolerance,
                                 const LineageType lineage,
                                 const uint8_t maxRetries,
                                 const z3::ContextPtr& z3Context,
                                 const QueryParsingServicePtr& queryParsingService)
    : AbstractRequest({ResourceType::QueryCatalogService,
                       ResourceType::GlobalExecutionPlan,
                       ResourceType::Topology,
                       ResourceType::GlobalQueryPlan,
                       ResourceType::UdfCatalog,
                       ResourceType::SourceCatalog},
                      maxRetries),
      queryString(queryString), queryPlan(nullptr), queryPlacementStrategy(queryPlacementStrategy),
      faultTolerance(faultTolerance), lineage(lineage), z3Context(z3Context), queryParsingService(queryParsingService) {}

AddQueryRequest::AddQueryRequest(const QueryPlanPtr& queryPlan,
                                 const Optimizer::PlacementStrategy queryPlacementStrategy,
                                 const FaultToleranceType faultTolerance,
                                 const LineageType lineage,
                                 const uint8_t maxRetries,
                                 const z3::ContextPtr& z3Context)
    : AbstractRequest({ResourceType::QueryCatalogService,
                       ResourceType::GlobalExecutionPlan,
                       ResourceType::Topology,
                       ResourceType::GlobalQueryPlan,
                       ResourceType::UdfCatalog,
                       ResourceType::SourceCatalog},
                      maxRetries),
      queryString(""), queryPlan(queryPlan), queryPlacementStrategy(queryPlacementStrategy), faultTolerance(faultTolerance),
      lineage(lineage), z3Context(z3Context), queryParsingService(nullptr) {}

AddQueryRequestPtr AddQueryRequest::create(const std::string& queryPlan,
                                           const Optimizer::PlacementStrategy queryPlacementStrategy,
                                           const FaultToleranceType faultTolerance,
                                           const LineageType lineage,
                                           const uint8_t maxRetries,
                                           const z3::ContextPtr& z3Context,
                                           const QueryParsingServicePtr& queryParsingService) {
    return std::make_shared<AddQueryRequest>(
        AddQueryRequest(queryPlan, queryPlacementStrategy, faultTolerance, lineage, maxRetries, z3Context, queryParsingService));
}

AddQueryRequestPtr AddQueryRequest::create(const QueryPlanPtr& queryPlan,
                                           const Optimizer::PlacementStrategy queryPlacementStrategy,
                                           const FaultToleranceType faultTolerance,
                                           const LineageType lineage,
                                           const uint8_t maxRetries,
                                           const z3::ContextPtr& z3Context) {
    return std::make_shared<AddQueryRequest>(
        AddQueryRequest(queryPlan, queryPlacementStrategy, faultTolerance, lineage, maxRetries, z3Context));
}

void AddQueryRequest::preRollbackHandle([[maybe_unused]] const RequestExecutionException& ex,
                                        [[maybe_unused]] StorageHandlerPtr storageHandler) {}

std::vector<AbstractRequestPtr> AddQueryRequest::rollBack([[maybe_unused]] RequestExecutionException& exception,
                                                          [[maybe_unused]] StorageHandlerPtr storageHandler) {
    try {
        NES_TRACE("Error: {}", exception.what());
        if (exception.instanceOf<Exceptions::QueryNotFoundException>()) {
            NES_ERROR("{}", exception.what());
        } else if (exception.instanceOf<Exceptions::InvalidQueryStateException>()
                   || exception.instanceOf<MapEntryNotFoundException>() || exception.instanceOf<TypeInferenceException>()
                   || exception.instanceOf<Exceptions::LogicalSourceNotFoundException>()
                   || exception.instanceOf<SignatureComputationException>()
                   || exception.instanceOf<Exceptions::PhysicalSourceNotFoundException>()
                   || exception.instanceOf<Exceptions::SharedQueryPlanNotFoundException>() || exception.instanceOf<UDFException>()
                   || exception.instanceOf<Exceptions::OperatorNotFoundException>()
                   || exception.instanceOf<Exceptions::InvalidLogicalOperatorException>()
                   || exception.instanceOf<GlobalQueryPlanUpdateException>()) {
            NES_ERROR("{}", exception.what());
            auto queryCatalogService = storageHandler->getQueryCatalogServiceHandle(requestId);
            queryCatalogService->updateQueryStatus(queryId,
                                                   QueryState::FAILED,
                                                   "Query failed due to " + std::string(exception.what()));
        } else if (exception.instanceOf<Exceptions::QueryPlacementException>()
                   || exception.instanceOf<Exceptions::ExecutionNodeNotFoundException>()
                   || exception.instanceOf<QueryDeploymentException>()) {
            NES_ERROR("{}", exception.what());
            auto globalQueryPlan = storageHandler->getGlobalQueryPlanHandle(requestId);
            globalQueryPlan->removeQuery(queryId, RequestType::FailQuery);
            auto queryCatalogService = storageHandler->getQueryCatalogServiceHandle(requestId);
            queryCatalogService->updateQueryStatus(queryId,
                                                   QueryState::FAILED,
                                                   "Query failed due to " + std::string(exception.what()));
        } else {
            NES_ERROR("Unknown exception: {}", exception.what());
        }
    } catch (RequestExecutionException& exception) {
        if (retry()) {
            handleError(exception, storageHandler);
        } else {
            NES_ERROR("StopQueryRequest: Final failure to rollback. No retries left. Error: {}", exception.what());
        }
    }
    return {};
}

void AddQueryRequest::postRollbackHandle([[maybe_unused]] const RequestExecutionException& exception,
                                         [[maybe_unused]] StorageHandlerPtr storageHandler) {}

void AddQueryRequest::postExecution([[maybe_unused]] StorageHandlerPtr storageHandler) {}

std::vector<AbstractRequestPtr> AddQueryRequest::executeRequestLogic(StorageHandlerPtr storageHandler) {
    try {
        NES_DEBUG("Acquiring required resources.");
        // Acquire all necessary resources
        auto globalExecutionPlan = storageHandler->getGlobalExecutionPlanHandle(requestId);
        auto topology = storageHandler->getTopologyHandle(requestId);
        auto queryCatalogService = storageHandler->getQueryCatalogServiceHandle(requestId);
        auto globalQueryPlan = storageHandler->getGlobalQueryPlanHandle(requestId);
        auto udfCatalog = storageHandler->getUDFCatalogHandle(requestId);
        auto sourceCatalog = storageHandler->getSourceCatalogHandle(requestId);
        auto coordinatorConfiguration = storageHandler->getCoordinatorConfiguration(requestId);

        NES_DEBUG("Initializing various optimization phases.");
        // Initialize all necessary phases
        auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
        auto queryPlacementPhase =
            Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, coordinatorConfiguration);
        auto queryDeploymentPhase =
            QueryDeploymentPhase::create(globalExecutionPlan, queryCatalogService, coordinatorConfiguration);
        auto queryUndeploymentPhase = QueryUndeploymentPhase::create(topology, globalExecutionPlan);
        auto optimizerConfigurations = coordinatorConfiguration->optimizer;
        auto queryMergerPhase = Optimizer::QueryMergerPhase::create(this->z3Context, optimizerConfigurations.queryMergerRule);
        typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, std::move(udfCatalog));
        auto sampleCodeGenerationPhase = Optimizer::SampleCodeGenerationPhase::create();
        auto queryRewritePhase = Optimizer::QueryRewritePhase::create(coordinatorConfiguration);
        auto originIdInferencePhase = Optimizer::OriginIdInferencePhase::create();
        auto topologySpecificQueryRewritePhase =
            Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, optimizerConfigurations);
        auto signatureInferencePhase =
            Optimizer::SignatureInferencePhase::create(this->z3Context, optimizerConfigurations.queryMergerRule);
        auto memoryLayoutSelectionPhase =
            Optimizer::MemoryLayoutSelectionPhase::create(optimizerConfigurations.memoryLayoutPolicy);
        auto syntacticQueryValidation = Optimizer::SyntacticQueryValidation::create(queryParsingService);
        auto semanticQueryValidation =
            Optimizer::SemanticQueryValidation::create(sourceCatalog,
                                                       coordinatorConfiguration->optimizer.performAdvanceSemanticValidation,
                                                       udfCatalog);

        // Compile and perform syntactic check if necessary
        if (!queryString.empty() && !queryPlan) {
            // Checking the syntactic validity and compiling the query string to an object
            queryPlan = syntacticQueryValidation->validate(queryString);
        } else if (queryPlan && queryString.empty()) {
            queryString = queryPlan->toString();
        } else {
            //FIXME: error handling
        }

        // Set unique identifier and additional properties to the query
        auto queryId = PlanIdGenerator::getNextQueryId();
        queryPlan->setQueryId(queryId);
        queryPlan->setPlacementStrategy(queryPlacementStrategy);
        queryPlan->setFaultToleranceType(faultTolerance);
        queryPlan->setLineageType(lineage);

        // Perform semantic validation
        semanticQueryValidation->validate(queryPlan);

        // respond to the calling service with the query id
        responsePromise.set_value(std::make_shared<AddQueryResponse>(queryId));

        // Create a new entry in the query catalog
        queryCatalogService->createNewEntry(queryString, queryPlan, queryPlacementStrategy);

        //1. Add the initial version of the query to the query catalog
        queryCatalogService->addUpdatedQueryPlan(queryId, "Input Query Plan", queryPlan);

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
    } catch (RequestExecutionException& exception) {
        handleError(exception, storageHandler);
    }
    return {};
}

}// namespace NES::Experimental
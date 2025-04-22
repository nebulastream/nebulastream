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
#include <Catalogs/Exceptions/LogicalSourceNotFoundException.hpp>
#include <Catalogs/Exceptions/PhysicalSourceNotFoundException.hpp>
#include <Catalogs/Exceptions/QueryNotFoundException.hpp>
#include <Catalogs/Query/QueryCatalog.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Exceptions/ExecutionNodeNotFoundException.hpp>
#include <Exceptions/MapEntryNotFoundException.hpp>
#include <Exceptions/QueryDeploymentException.hpp>
#include <Operators/Exceptions/InvalidLogicalOperatorException.hpp>
#include <Operators/Exceptions/SignatureComputationException.hpp>
#include <Operators/Exceptions/TypeInferenceException.hpp>
#include <Operators/Exceptions/UDFException.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperator.hpp>
#include <Optimizer/Exceptions/GlobalQueryPlanUpdateException.hpp>
#include <Optimizer/Exceptions/OperatorNotFoundException.hpp>
#include <Optimizer/Exceptions/QueryPlacementAdditionException.hpp>
#include <Optimizer/Exceptions/SharedQueryPlanNotFoundException.hpp>
#include <Optimizer/Phases/MemoryLayoutSelectionPhase.hpp>
#include <Optimizer/Phases/OriginIdInferencePhase.hpp>
#include <Optimizer/Phases/QueryMergerPhase.hpp>
#include <Optimizer/Phases/QueryPlacementAmendmentPhase.hpp>
#include <Optimizer/Phases/QueryRewritePhase.hpp>
#include <Optimizer/Phases/SignatureInferencePhase.hpp>
#include <Optimizer/Phases/StatisticIdInferencePhase.hpp>
#include <Optimizer/Phases/TopologySpecificQueryRewritePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Phases/DeploymentPhase.hpp>
#include <Phases/SampleCodeGenerationPhase.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Utils/PlanIdGenerator.hpp>
#include <Plans/Utils/PlanIterator.hpp>
#include <QueryValidation/SemanticQueryValidation.hpp>
#include <QueryValidation/SyntacticQueryValidation.hpp>
#include <RequestProcessor/RequestTypes/AddQueryRequest.hpp>
#include <RequestProcessor/StorageHandles/ResourceType.hpp>
#include <RequestProcessor/StorageHandles/StorageHandler.hpp>
#include <Services/PlacementAmendment/PlacementAmendmentHandler.hpp>
#include <Services/PlacementAmendment/PlacementAmendmentInstance.hpp>
#include <Services/QueryParsingService.hpp>
#include <Util/DeploymentContext.hpp>
#include <Util/FaultToleranceType.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Placement/PlacementStrategy.hpp>
#include <string>
#include <utility>
#include <Util/CheckpointStorageType.hpp>

namespace NES::RequestProcessor {

AddQueryRequest::AddQueryRequest(const std::string& queryString,
                                 const Optimizer::PlacementStrategy queryPlacementStrategy,
                                 const uint8_t maxRetries,
                                 const z3::ContextPtr& z3Context,
                                 const QueryParsingServicePtr& queryParsingService,
                                 const Optimizer::PlacementAmendmentHandlerPtr& placementAmendmentHandler,
                                 FaultToleranceType faultTolerance,
                                 CheckpointStorageType checkpointStorage)
    : AbstractUniRequest({ResourceType::QueryCatalogService,
                          ResourceType::GlobalExecutionPlan,
                          ResourceType::Topology,
                          ResourceType::GlobalQueryPlan,
                          ResourceType::UdfCatalog,
                          ResourceType::SourceCatalog,
                          ResourceType::CoordinatorConfiguration,
                          ResourceType::StatisticProbeHandler},
                         maxRetries),
      queryId(INVALID_QUERY_ID), queryString(queryString), queryPlan(nullptr), queryPlacementStrategy(queryPlacementStrategy),
      z3Context(z3Context), queryParsingService(queryParsingService), placementAmendmentHandler(placementAmendmentHandler),
      faultTolerance(faultTolerance), checkpointStorage(checkpointStorage) {}

AddQueryRequest::AddQueryRequest(const QueryPlanPtr& queryPlan,
                                 const Optimizer::PlacementStrategy queryPlacementStrategy,
                                 const uint8_t maxRetries,
                                 const z3::ContextPtr& z3Context,
                                 const Optimizer::PlacementAmendmentHandlerPtr& placementAmendmentHandler,
                                 FaultToleranceType faultTolerance,
                                 CheckpointStorageType checkpointStorage)
    : AbstractUniRequest({ResourceType::QueryCatalogService,
                          ResourceType::GlobalExecutionPlan,
                          ResourceType::Topology,
                          ResourceType::GlobalQueryPlan,
                          ResourceType::UdfCatalog,
                          ResourceType::SourceCatalog,
                          ResourceType::CoordinatorConfiguration,
                          ResourceType::StatisticProbeHandler},
                         maxRetries),
      queryId(INVALID_QUERY_ID), queryString(""), queryPlan(queryPlan), queryPlacementStrategy(queryPlacementStrategy),
      z3Context(z3Context), queryParsingService(nullptr), placementAmendmentHandler(placementAmendmentHandler),
      faultTolerance(faultTolerance), checkpointStorage(checkpointStorage) {}

AddQueryRequestPtr AddQueryRequest::create(const std::string& queryPlan,
                                           const Optimizer::PlacementStrategy queryPlacementStrategy,
                                           const uint8_t maxRetries,
                                           const z3::ContextPtr& z3Context,
                                           const QueryParsingServicePtr& queryParsingService,
                                           const Optimizer::PlacementAmendmentHandlerPtr& placementAmendmentHandler,
                                           FaultToleranceType faultTolerance,
                                           CheckpointStorageType checkpointStorage) {
    return std::make_shared<AddQueryRequest>(queryPlan,
                                             queryPlacementStrategy,
                                             maxRetries,
                                             z3Context,
                                             queryParsingService,
                                             placementAmendmentHandler,
                                             faultTolerance,
                                             checkpointStorage);
}

AddQueryRequestPtr AddQueryRequest::create(const QueryPlanPtr& queryPlan,
                                           const Optimizer::PlacementStrategy queryPlacementStrategy,
                                           const uint8_t maxRetries,
                                           const z3::ContextPtr& z3Context,
                                           const Optimizer::PlacementAmendmentHandlerPtr& placementAmendmentHandler,
                                           FaultToleranceType faultTolerance,
                                           CheckpointStorageType checkpointStorage) {
    return std::make_shared<AddQueryRequest>(queryPlan,
                                             queryPlacementStrategy,
                                             maxRetries,
                                             z3Context,
                                             placementAmendmentHandler,
                                             faultTolerance,
                                             checkpointStorage);
}

void AddQueryRequest::preRollbackHandle([[maybe_unused]] std::exception_ptr ex,
                                        [[maybe_unused]] const StorageHandlerPtr& storageHandler) {}

std::vector<AbstractRequestPtr> AddQueryRequest::rollBack([[maybe_unused]] std::exception_ptr exception,
                                                          [[maybe_unused]] const StorageHandlerPtr& storageHandler) {
    try {
        std::rethrow_exception(exception);
    } catch (Exceptions::QueryNotFoundException& e) {
    } catch (Exceptions::InvalidQueryStateException& e) {
        markAsFailedInQueryCatalog(e, storageHandler);
    } catch (MapEntryNotFoundException& e) {
        markAsFailedInQueryCatalog(e, storageHandler);
    } catch (TypeInferenceException& e) {
        markAsFailedInQueryCatalog(e, storageHandler);
    } catch (Exceptions::LogicalSourceNotFoundException& e) {
        markAsFailedInQueryCatalog(e, storageHandler);
    } catch (SignatureComputationException& e) {
        markAsFailedInQueryCatalog(e, storageHandler);
    } catch (Exceptions::PhysicalSourceNotFoundException& e) {
        markAsFailedInQueryCatalog(e, storageHandler);
    } catch (Exceptions::SharedQueryPlanNotFoundException& e) {
        markAsFailedInQueryCatalog(e, storageHandler);
    } catch (UDFException& e) {
        markAsFailedInQueryCatalog(e, storageHandler);
    } catch (Exceptions::OperatorNotFoundException& e) {
        markAsFailedInQueryCatalog(e, storageHandler);
    } catch (Exceptions::InvalidLogicalOperatorException& e) {
        markAsFailedInQueryCatalog(e, storageHandler);
    } catch (GlobalQueryPlanUpdateException& e) {
        markAsFailedInQueryCatalog(e, storageHandler);
    } catch (Exceptions::QueryPlacementAdditionException& e) {
        //todo #4296: remove from global execution plan as well
        removeFromGlobalQueryPlanAndMarkAsFailed(e, storageHandler);
    } catch (Exceptions::ExecutionNodeNotFoundException& e) {
        //todo #4296: remove from global execution plan as well
        removeFromGlobalQueryPlanAndMarkAsFailed(e, storageHandler);
    } catch (QueryDeploymentException& e) {
        //todo #4296: remove from global execution plan as well
        removeFromGlobalQueryPlanAndMarkAsFailed(e, storageHandler);
    } catch (Exceptions::RequestExecutionException& e) {
        setExceptionInPromiseOrRethrow(exception);
    }

    //make sure the promise is set before returning in case a the caller is waiting on it
    trySetExceptionInPromise(
        std::make_exception_ptr<RequestExecutionException>(RequestExecutionException("No return value set in promise")));
    return {};
}

void AddQueryRequest::removeFromGlobalQueryPlanAndMarkAsFailed(std::exception& e, const StorageHandlerPtr& storageHandler) {
    auto globalQueryPlan = storageHandler->getGlobalQueryPlanHandle(requestId);
    globalQueryPlan->removeQuery(queryId, RequestType::FailQuery);
    markAsFailedInQueryCatalog(e, storageHandler);
}

void AddQueryRequest::markAsFailedInQueryCatalog(std::exception& e, const StorageHandlerPtr& storageHandler) {
    auto queryCatalog = storageHandler->getQueryCatalogHandle(requestId);
    queryCatalog->updateQueryStatus(queryId, QueryState::FAILED, e.what());
}

void AddQueryRequest::postRollbackHandle([[maybe_unused]] std::exception_ptr exception,
                                         [[maybe_unused]] const StorageHandlerPtr& storageHandler) {}

std::vector<AbstractRequestPtr> AddQueryRequest::executeRequestLogic(const StorageHandlerPtr& storageHandler) {
    try {
        NES_DEBUG("Acquiring required resources.");
        // Acquire all necessary resources
        auto globalExecutionPlan = storageHandler->getGlobalExecutionPlanHandle(requestId);
        auto topology = storageHandler->getTopologyHandle(requestId);
        auto queryCatalog = storageHandler->getQueryCatalogHandle(requestId);
        auto globalQueryPlan = storageHandler->getGlobalQueryPlanHandle(requestId);
        auto udfCatalog = storageHandler->getUDFCatalogHandle(requestId);
        auto sourceCatalog = storageHandler->getSourceCatalogHandle(requestId);
        auto coordinatorConfiguration = storageHandler->getCoordinatorConfiguration(requestId);
        auto statisticProbeHandler = storageHandler->getStatisticProbeHandler(requestId);

        NES_DEBUG("Initializing various optimization phases.");
        // Initialize all necessary phases
        auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
        auto queryPlacementAmendmentPhase = Optimizer::QueryPlacementAmendmentPhase::create(globalExecutionPlan,
                                                                                            topology,
                                                                                            typeInferencePhase,
                                                                                            coordinatorConfiguration);
        auto optimizerConfigurations = coordinatorConfiguration->optimizer;
        auto queryMergerPhase = Optimizer::QueryMergerPhase::create(this->z3Context, optimizerConfigurations);
        typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, std::move(udfCatalog));
        auto sampleCodeGenerationPhase = Optimizer::SampleCodeGenerationPhase::create();
        auto queryRewritePhase = Optimizer::QueryRewritePhase::create(coordinatorConfiguration);
        auto originIdInferencePhase = Optimizer::OriginIdInferencePhase::create();
        auto statisticIdInferencePhase = Optimizer::StatisticIdInferencePhase::create();
        auto topologySpecificQueryRewritePhase = Optimizer::TopologySpecificQueryRewritePhase::create(topology,
                                                                                                      sourceCatalog,
                                                                                                      optimizerConfigurations,
                                                                                                      statisticProbeHandler);
        auto signatureInferencePhase =
            Optimizer::SignatureInferencePhase::create(this->z3Context, optimizerConfigurations.queryMergerRule);
        auto memoryLayoutSelectionPhase =
            Optimizer::MemoryLayoutSelectionPhase::create(optimizerConfigurations.memoryLayoutPolicy);
        auto syntacticQueryValidation = Optimizer::SyntacticQueryValidation::create(queryParsingService);
        auto semanticQueryValidation =
            Optimizer::SemanticQueryValidation::create(sourceCatalog,
                                                       udfCatalog,
                                                       coordinatorConfiguration->optimizer.performAdvanceSemanticValidation);

        // Compile and perform syntactic check if necessary
        if (!queryString.empty() && !queryPlan) {
            // Checking the syntactic validity and compiling the query string to an object
            queryPlan = syntacticQueryValidation->validate(queryString);
        } else if (queryPlan && queryString.empty()) {
            // assign unique operator identifier to the operators in the query plan
            assignOperatorIds(queryPlan);
            queryString = queryPlan->toString();
        } else {
            NES_ERROR("Please supply either query string or query plan while creating this request.");
            NES_NOT_IMPLEMENTED();
        }

        // Set unique identifier and additional properties to the query
        queryId = PlanIdGenerator::getNextQueryId();
        queryPlan->setQueryId(queryId);
        queryPlan->setPlacementStrategy(queryPlacementStrategy);
        queryPlan->setFaultTolerance(faultTolerance);
        queryPlan->setCheckpointStorage(checkpointStorage);

        // Perform semantic validation
        semanticQueryValidation->validate(queryPlan);

        // Create a new entry in the query catalog
        queryCatalog->createQueryCatalogEntry(queryString, queryPlan, queryPlacementStrategy, QueryState::REGISTERED);

        // respond to the calling service with the query id after creating an entry in the catalog
        responsePromise.set_value(std::make_shared<AddQueryResponse>(queryId));

        //1. Add the initial version of the query to the query catalog
        queryCatalog->addUpdatedQueryPlan(queryId, "Input Query Plan", queryPlan);

        //2. Set query status as Optimizing
        queryCatalog->updateQueryStatus(queryId, QueryState::OPTIMIZING, "");

        //3. Execute type inference phase
        NES_DEBUG("Performing Query type inference phase for query:  {}", queryId);
        queryPlan = typeInferencePhase->execute(queryPlan, faultTolerance);

        //4. Set memory layout of each logical operator
        NES_DEBUG("Performing query choose memory layout phase:  {}", queryId);
        queryPlan = memoryLayoutSelectionPhase->execute(queryPlan);

        //5. Perform query re-write
        NES_DEBUG("Performing Query rewrite phase for query:  {}", queryId);
        queryPlan = queryRewritePhase->execute(queryPlan);

        //6. Add the updated query plan to the query catalog
        queryCatalog->addUpdatedQueryPlan(queryId, "Query Rewrite Phase", queryPlan);

        //7. Execute type inference phase on rewritten query plan
        queryPlan = typeInferencePhase->execute(queryPlan, faultTolerance);

        //8. Generate sample code for elegant planner
        if (queryPlacementStrategy == Optimizer::PlacementStrategy::ELEGANT_BALANCED
            || queryPlacementStrategy == Optimizer::PlacementStrategy::ELEGANT_PERFORMANCE
            || queryPlacementStrategy == Optimizer::PlacementStrategy::ELEGANT_ENERGY) {
            queryPlan = sampleCodeGenerationPhase->execute(queryPlan);
        }

        //9. Perform signature inference phase for sharing identification among query plans
        signatureInferencePhase->execute(queryPlan);

        //10. Assign a unique statisticId to all logical operators
        queryPlan = statisticIdInferencePhase->execute(queryPlan);

        //11. Perform topology specific rewrites to the query plan
        queryPlan = topologySpecificQueryRewritePhase->execute(queryPlan);

        //12. Add the updated query plan to the query catalog
        queryCatalog->addUpdatedQueryPlan(queryId, "Topology Specific Query Rewrite Phase", queryPlan);

        //13. Perform type inference over re-written query plan
        queryPlan = typeInferencePhase->execute(queryPlan, faultTolerance);

        //14. Identify the number of origins and their ids for all logical operators
        queryPlan = originIdInferencePhase->execute(queryPlan);

        //15. Set memory layout of each logical operator in the rewritten query
        NES_DEBUG("Performing query choose memory layout phase:  {}", queryId);
        queryPlan = memoryLayoutSelectionPhase->execute(queryPlan);

        //16. Add the updated query plan to the query catalog
        queryCatalog->addUpdatedQueryPlan(queryId, "Executed Query Plan", queryPlan);

        //17. Add the updated query plan to the global query plan
        NES_DEBUG("Performing Query type inference phase for query:  {}", queryId);
        globalQueryPlan->addQueryPlan(queryPlan);

        //18. Perform query merging for newly added query plan
        NES_DEBUG("Applying Query Merger Rules as Query Merging is enabled.");
        queryMergerPhase->execute(globalQueryPlan);

        //19. Get the shared query plan id for the added query
        auto sharedQueryId = globalQueryPlan->getSharedQueryId(queryId);
        if (sharedQueryId == INVALID_SHARED_QUERY_ID) {
            throw Exceptions::SharedQueryPlanNotFoundException(
                "Could not find shared query id in global query plan. Shared query id is invalid.",
                sharedQueryId);
        }

        //20. Get the shared query plan for the added query
        auto sharedQueryPlan = globalQueryPlan->getSharedQueryPlan(sharedQueryId);
        if (!sharedQueryPlan) {
            throw Exceptions::SharedQueryPlanNotFoundException("Could not obtain shared query plan by shared query id.",
                                                               sharedQueryId);
        }

        if (sharedQueryPlan->getStatus() == SharedQueryPlanStatus::CREATED) {
            queryCatalog->createSharedQueryCatalogEntry(sharedQueryId, {queryId}, QueryState::OPTIMIZING);
        } else {
            queryCatalog->updateSharedQueryStatus(sharedQueryId, QueryState::OPTIMIZING, "");
        }
        //Link both catalogs
        queryCatalog->linkSharedQuery(queryId, sharedQueryId);

        //21. Perform placement amendment and deployment of updated shared query plan
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
    } catch (RequestExecutionException& exception) {
        NES_ERROR("Exception occurred while processing AddQueryRequest with error {}", exception.what());
        handleError(std::current_exception(), storageHandler);
    }
    return {};
}

void AddQueryRequest::assignOperatorIds(const QueryPlanPtr& queryPlan) {
    // Iterate over all operators in the query and replace the client-provided ID
    auto queryPlanIterator = PlanIterator(queryPlan);
    for (auto itr = queryPlanIterator.begin(); itr != PlanIterator::end(); ++itr) {
        auto visitingOp = (*itr)->as<Operator>();
        visitingOp->setId(NES::getNextOperatorId());
    }
}

}// namespace NES::RequestProcessor

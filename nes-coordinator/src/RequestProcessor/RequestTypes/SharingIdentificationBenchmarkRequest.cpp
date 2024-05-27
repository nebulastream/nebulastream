#include <RequestProcessor/RequestTypes/SharingIdentificationBenchmarkRequest.hpp>

#include <Catalogs/Query/QueryCatalog.hpp>
#include <Catalogs/Query/QueryCatalogEntry.hpp>
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Components/NesCoordinator.hpp>
#include <Configurations/WorkerConfigurationKeys.hpp>
#include <Configurations/WorkerPropertyKeys.hpp>
#include <Exceptions/ExecutionNodeNotFoundException.hpp>
#include <Exceptions/QueryDeploymentException.hpp>
#include <Exceptions/QueryUndeploymentException.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/PlanIterator.hpp>
#include <RequestProcessor/StorageHandles/ResourceType.hpp>
#include <RequestProcessor/StorageHandles/StorageHandler.hpp>
#include <Services/QueryParsingService.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <Util/yaml/Yaml.hpp>
#include <Version/version.hpp>
#include <fstream>
#include <unistd.h>

#include <Catalogs/Exceptions/InvalidQueryStateException.hpp>
#include <Catalogs/Exceptions/LogicalSourceNotFoundException.hpp>
#include <Catalogs/Exceptions/PhysicalSourceNotFoundException.hpp>
#include <Catalogs/Exceptions/QueryNotFoundException.hpp>
#include <Catalogs/Query/QueryCatalog.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Util/PlanJsonGenerator.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Exceptions/ExecutionNodeNotFoundException.hpp>
#include <Exceptions/MapEntryNotFoundException.hpp>
#include <Exceptions/QueryDeploymentException.hpp>
#include <Operators/Exceptions/InvalidLogicalOperatorException.hpp>
#include <Operators/Exceptions/SignatureComputationException.hpp>
#include <Operators/Exceptions/TypeInferenceException.hpp>
#include <Operators/Exceptions/UDFException.hpp>
#include <Operators/LogicalOperators/Network/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Network/NetworkSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperator.hpp>
#include <Optimizer/Exceptions/GlobalQueryPlanUpdateException.hpp>
#include <Optimizer/Exceptions/OperatorNotFoundException.hpp>
#include <Optimizer/Exceptions/QueryPlacementAdditionException.hpp>
#include <Optimizer/Exceptions/SharedQueryPlanNotFoundException.hpp>
#include <Optimizer/Phases/MemoryLayoutSelectionPhase.hpp>
#include <Optimizer/Phases/OriginIdInferencePhase.hpp>
#include <Optimizer/Phases/PlacementAmendment/QueryPlacementAmendmentPhase.hpp>
#include <Optimizer/Phases/QueryMergerPhase.hpp>
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
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/PlanIdGenerator.hpp>
#include <Plans/Utils/PlanIterator.hpp>
#include <QueryValidation/SemanticQueryValidation.hpp>
#include <QueryValidation/SyntacticQueryValidation.hpp>
#include <RequestProcessor/RequestTypes/AddQueryRequest.hpp>
#include <RequestProcessor/StorageHandles/ResourceType.hpp>
#include <RequestProcessor/StorageHandles/StorageHandler.hpp>
#include <Services/QueryParsingService.hpp>
#include <Util/DeploymentContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Placement/PlacementStrategy.hpp>
#include <string>
#include <utility>

// TODO: reorganize header files

namespace {

using SourceCatalogPtr = std::shared_ptr<NES::Catalogs::Source::SourceCatalog>;

}// namespace

namespace NES::RequestProcessor {

SharingIdentificationBenchmarkRequest::SharingIdentificationBenchmarkRequest(
    const std::vector<std::string>& queryStrings,
    const Optimizer::QueryMergerRule queryMergerRule,
    const Optimizer::PlacementStrategy queryPlacementStrategy,
    const uint8_t maxRetries,
    const z3::ContextPtr& z3Context,
    const QueryParsingServicePtr& queryParsingService,
    const bool deploy)
    : AbstractUniRequest({ResourceType::QueryCatalogService,
                          ResourceType::GlobalExecutionPlan,
                          ResourceType::Topology,
                          ResourceType::GlobalQueryPlan,
                          ResourceType::UdfCatalog,
                          ResourceType::SourceCatalog,
                          ResourceType::CoordinatorConfiguration},
                         maxRetries),
      queryStrings(queryStrings), queryMergerRule(queryMergerRule), queryPlacementStrategy(queryPlacementStrategy),
      z3Context(z3Context), queryParsingService(queryParsingService), deploy(deploy) {}

SharingIdentificationBenchmarkRequestPtr
SharingIdentificationBenchmarkRequest::create(const std::vector<std::string>& queryStrings,
                                              const Optimizer::QueryMergerRule queryMergerRule,
                                              const Optimizer::PlacementStrategy queryPlacementStrategy,
                                              const uint8_t maxRetries,
                                              const z3::ContextPtr& z3Context,
                                              const QueryParsingServicePtr& queryParsingService,
                                              const bool deploy) {

    return std::make_shared<SharingIdentificationBenchmarkRequest>(queryStrings,
                                                                   queryMergerRule,
                                                                   queryPlacementStrategy,
                                                                   maxRetries,
                                                                   z3Context,
                                                                   queryParsingService,
                                                                   deploy);
}

void SharingIdentificationBenchmarkRequest::preRollbackHandle([[maybe_unused]] std::exception_ptr ex,
                                                              [[maybe_unused]] const StorageHandlerPtr& storageHandler) {}

std::vector<AbstractRequestPtr>
SharingIdentificationBenchmarkRequest::rollBack([[maybe_unused]] std::exception_ptr exception,
                                                [[maybe_unused]] const StorageHandlerPtr& storageHandler) {
    return {};
}

void SharingIdentificationBenchmarkRequest::postRollbackHandle([[maybe_unused]] std::exception_ptr exception,
                                                               [[maybe_unused]] const StorageHandlerPtr& storageHandler) {}

void setupPhysicalSources(SourceCatalogPtr sourceCatalog, uint64_t noOfPhysicalSource = 1) {//TODO: Call once
    for (uint64_t j = 0; j < 4; j++) {                                                      //TODO: parameterize
        LogicalSourcePtr logicalSource = sourceCatalog->getLogicalSource("example" + std::to_string(j + 1));
        for (uint64_t i = 1; i <= noOfPhysicalSource; i++) {
            //Create physical source
            auto physicalSource =
                PhysicalSource::create("example" + std::to_string(j + 1), "example" + std::to_string(j + 1) + std::to_string(i));
            auto sce = Catalogs::Source::SourceCatalogEntry::create(physicalSource, logicalSource, WorkerId(i));//？workerID!!!!
            sourceCatalog->addPhysicalSource("example" + std::to_string(j + 1), sce);
        }
    }
}

std::vector<AbstractRequestPtr>
SharingIdentificationBenchmarkRequest::executeRequestLogic(const StorageHandlerPtr& storageHandler) {
    try {
        auto startTime =
            std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

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
        auto deploymentPhase = DeploymentPhase::create(queryCatalog);
        auto optimizerConfigurations = coordinatorConfiguration->optimizer;
        optimizerConfigurations.queryMergerRule = queryMergerRule;
        auto queryMergerPhase = Optimizer::QueryMergerPhase::create(this->z3Context, optimizerConfigurations);
        typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, std::move(udfCatalog));
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

        setupPhysicalSources(sourceCatalog);

        uint64_t totalOperators = 0;
        std::vector<QueryId> queryIds = {};

        for (const auto& queryString : queryStrings) {
            // Compile and perform syntactic check if necessary
            if (!queryString.empty()) {
                // Checking the syntactic validity and compiling the query string to an object
                queryPlan = syntacticQueryValidation->validate(queryString);
            } else {
                NES_ERROR("Please supply query string while creating this request.");
                NES_NOT_IMPLEMENTED();
            }

            // Set unique identifier and additional properties to the query
            const auto queryId = PlanIdGenerator::getNextQueryId();
            queryIds.push_back(queryId);
            queryPlan->setQueryId(queryId);
            queryPlan->setPlacementStrategy(queryPlacementStrategy);

            // Perform semantic validation
            semanticQueryValidation->validate(queryPlan);

            // Create a new entry in the query catalog
            queryCatalog->createQueryCatalogEntry(queryString, queryPlan, queryPlacementStrategy, QueryState::REGISTERED);

            //1. Add the initial version of the query to the query catalog
            queryCatalog->addUpdatedQueryPlan(queryId, "Input Query Plan", queryPlan);

            //2. Set query status as Optimizing
            queryCatalog->updateQueryStatus(queryId, QueryState::OPTIMIZING, "");

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
            queryCatalog->addUpdatedQueryPlan(queryId, "Query Rewrite Phase", queryPlan);

            //7. Execute type inference phase on rewritten query plan
            NES_DEBUG("Performing Type inference phase for rewritten query:  {}", queryId);
            queryPlan = typeInferencePhase->execute(queryPlan);

            //9. Perform signature inference phase for sharing identification among query plans
            NES_DEBUG("Perform signature inference phase for sharing identification among query plans:  {}", queryId);
            signatureInferencePhase->execute(queryPlan);

            //10. Assign a unique statisticId to all logical operators
            queryPlan = statisticIdInferencePhase->execute(queryPlan);

            //11. Perform topology specific rewrites to the query plan
            NES_DEBUG("Perform topology specific rewrites to the query plan:  {}", queryId);
            queryPlan = topologySpecificQueryRewritePhase->execute(queryPlan);

            //12. Add the updated query plan to the query catalog
            queryCatalog->addUpdatedQueryPlan(queryId, "Topology Specific Query Rewrite Phase", queryPlan);

            //13. Perform type inference over re-written query plan
            NES_DEBUG("Perform type inference over re-written query plan:  {}", queryId);
            queryPlan = typeInferencePhase->execute(queryPlan);

            //14. Identify the number of origins and their ids for all logical operators
            queryPlan = originIdInferencePhase->execute(queryPlan);

            //15. Set memory layout of each logical operator in the rewritten query
            NES_DEBUG("Performing query choose memory layout phase:  {}", queryId);
            queryPlan = memoryLayoutSelectionPhase->execute(queryPlan);

            //16. Add the updated query plan to the query catalog
            queryCatalog->addUpdatedQueryPlan(queryId, "Executed Query Plan", queryPlan);

            //compute totalOperators before merge
            totalOperators = totalOperators + PlanIterator(queryPlan).snapshot().size();

            //17. Add the updated query plan to the global query plan
            NES_DEBUG("Performing Query type inference phase for query:  {}", queryId);
            globalQueryPlan->addQueryPlan(queryPlan);

        }//end of for-loop

        //18. Perform query merging for newly added query plan
        NES_DEBUG("Applying Query Merger Rules as Query Merging is enabled.");
        queryMergerPhase->execute(globalQueryPlan);

        auto endTime =
            std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

        auto allSQP = globalQueryPlan->getAllSharedQueryPlans();

        uint64_t mergedOperators = 0;
        for (auto& sqp : allSQP) {
            unsigned long planSize = PlanIterator(sqp->getQueryPlan()).snapshot().size();
            mergedOperators = mergedOperators + planSize;
        }

        //Compute efficiency
        float efficiency = (((float) totalOperators - (float) mergedOperators) / (float) totalOperators) * 100.0;

        //Compute optimizationTime
        auto optimizationTime = endTime - startTime;

        //get Response as Json
        auto response = getResAsJson(allSQP, efficiency, optimizationTime);

        // respond to the calling service with the needed data
        responsePromise.set_value(std::make_shared<BenchmarkQueryResponse>(response));

        if (!deploy)
            return {};

        for (const auto queryId : queryIds) {
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

            //21. Perform placement of updated shared query plan
            NES_DEBUG("Performing Operator placement for shared query plan");
            auto deploymentContexts = queryPlacementAmendmentPhase->execute(sharedQueryPlan);

            //22. Perform deployment of re-placed shared query plan
            deploymentPhase->execute(deploymentContexts, RequestType::AddQuery);

            //23. Update the shared query plan as deployed
            sharedQueryPlan->setStatus(SharedQueryPlanStatus::DEPLOYED);

            // Iterate over deployment context and update execution plan
            for (const auto& deploymentContext : deploymentContexts) {
                auto WorkerId = deploymentContext->getWorkerId();
                auto decomposedQueryPlanId = deploymentContext->getDecomposedQueryPlanId();
                auto decomposedQueryPlanVersion = deploymentContext->getDecomposedQueryPlanVersion();
                auto decomposedQueryPlanState = deploymentContext->getDecomposedQueryPlanState();
                switch (decomposedQueryPlanState) {
                    case QueryState::MARKED_FOR_REDEPLOYMENT:
                    case QueryState::MARKED_FOR_DEPLOYMENT: {
                        globalExecutionPlan->updateDecomposedQueryPlanState(WorkerId,
                                                                            sharedQueryId,
                                                                            decomposedQueryPlanId,
                                                                            decomposedQueryPlanVersion,
                                                                            QueryState::RUNNING);
                        break;
                    }
                    case QueryState::MARKED_FOR_MIGRATION: {
                        globalExecutionPlan->updateDecomposedQueryPlanState(WorkerId,
                                                                            sharedQueryId,
                                                                            decomposedQueryPlanId,
                                                                            decomposedQueryPlanVersion,
                                                                            QueryState::STOPPED);
                        globalExecutionPlan->removeDecomposedQueryPlan(WorkerId,
                                                                       sharedQueryId,
                                                                       decomposedQueryPlanId,
                                                                       decomposedQueryPlanVersion);
                        break;
                    }
                    default:
                        NES_WARNING("Unhandled Deployment context with status: {}",
                                    magic_enum::enum_name(decomposedQueryPlanState));
                }
            }
        }
    } catch (RequestExecutionException& exception) {
        NES_ERROR("Exception occurred while processing AddQueryRequest with error {}", exception.what());
        handleError(std::current_exception(), storageHandler);
    }

    return {};
}

nlohmann::json mergeSharedQueryPlanJson(const std::vector<nlohmann::json>& sharedQueryPlanJsons) {
    nlohmann::json result{};
    auto merge = [&](const std::string& elementName) {
        std::vector<nlohmann::json> elements{};
        for (const auto& item : sharedQueryPlanJsons) {
            const auto& toInsert = item[elementName];
            std::copy(toInsert.begin(), toInsert.end(), std::back_inserter(elements));
        }
        return elements;
    };
    result["nodes"] = merge("nodes");
    result["edges"] = merge("edges");
    return result;
}

nlohmann::json
SharingIdentificationBenchmarkRequest::getResAsJson(std::vector<SharedQueryPlanPtr> allSQP, float efficiency, long optTime) {
    NES_INFO("UtilityFunctions: getting all shared query plan,sharingEfficiency,optimizationTime as JSON");

    nlohmann::json resJson = {};

    //convert allSharedQueryPlan format:  std::vector<SharedQueryPlanPtr>  to  std::vector<nlohmann::json>
    std::vector<nlohmann::json> sharedQueryPlans;
    for (auto& sharedQueryPlan : allSQP) {
        const auto& queryPlan = sharedQueryPlan->getQueryPlan();
        auto sharedQueryPlanJson = PlanJsonGenerator::getSharedQueryPlanAsJson(queryPlan);
        sharedQueryPlans.push_back(sharedQueryPlanJson);
    }

    resJson["sharedQueryPlans"] = mergeSharedQueryPlanJson(sharedQueryPlans);
    resJson["sharingEfficiency"] = efficiency;
    resJson["optimizationTime"] = optTime;

    return resJson;
}

}// namespace NES::RequestProcessor

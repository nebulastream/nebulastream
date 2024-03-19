#include <RequestProcessor/RequestTypes/SharingIdentificationBenchmarkRequest.hpp>

#include <Catalogs/Query/QueryCatalog.hpp>
#include <Catalogs/Query/QueryCatalogEntry.hpp>
#include <Catalogs/Query/QueryCatalogService.hpp>
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
#include <Services/RequestHandlerService.hpp>
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
#include <Catalogs/Query/QueryCatalogService.hpp>
#include <Catalogs/Topology/Topology.hpp>
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
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
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
#include <Optimizer/Phases/TopologySpecificQueryRewritePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Phases/GlobalQueryPlanUpdatePhase.hpp>
#include <Phases/QueryDeploymentPhase.hpp>
#include <Phases/QueryUndeploymentPhase.hpp>
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
#include <Util/Logger/Logger.hpp>
#include <Util/Placement/PlacementStrategy.hpp>
#include <Util/PlanJsonGenerator.hpp>
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
    const QueryParsingServicePtr& queryParsingService)
    : AbstractUniRequest({ResourceType::QueryCatalogService,
                          ResourceType::GlobalExecutionPlan,
                          ResourceType::Topology,
                          ResourceType::GlobalQueryPlan,
                          ResourceType::UdfCatalog,
                          ResourceType::SourceCatalog,
                          ResourceType::CoordinatorConfiguration},
                         maxRetries),
      queryStrings(queryStrings), queryMergerRule(queryMergerRule),
      queryPlacementStrategy(queryPlacementStrategy), z3Context(z3Context), queryParsingService(queryParsingService) {}

SharingIdentificationBenchmarkRequestPtr
SharingIdentificationBenchmarkRequest::create(const std::vector<std::string>& queryStrings,
                                              const Optimizer::QueryMergerRule queryMergerRule,
                                              const Optimizer::PlacementStrategy queryPlacementStrategy,
                                              const uint8_t maxRetries,
                                              const z3::ContextPtr& z3Context,
                                              const QueryParsingServicePtr& queryParsingService) {

    return std::make_shared<SharingIdentificationBenchmarkRequest>(queryStrings,
                                                                   queryMergerRule,
                                                                   queryPlacementStrategy,
                                                                   maxRetries,
                                                                   z3Context,
                                                                   queryParsingService);
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


void setupPhysicalSources(SourceCatalogPtr sourceCatalog, uint64_t noOfPhysicalSource=1) { //TODO
    for (uint64_t j = 0; j < 4; j++) { //TODO: parameterize
        LogicalSourcePtr logicalSource = sourceCatalog->getLogicalSource("example" + std::to_string(j + 1));
        for (uint64_t i = 1; i <= noOfPhysicalSource; i++) {
            //Create physical source
            auto physicalSource =
                PhysicalSource::create("example" + std::to_string(j + 1), "example" + std::to_string(j + 1) + std::to_string(i));
            auto sce = Catalogs::Source::SourceCatalogEntry::create(physicalSource, logicalSource, i);
            sourceCatalog->addPhysicalSource("example" + std::to_string(j + 1), sce);
        }
    }
}


std::vector<AbstractRequestPtr>
SharingIdentificationBenchmarkRequest::executeRequestLogic(const StorageHandlerPtr& storageHandler) {


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

        auto optimizerConfigurations = coordinatorConfiguration->optimizer;
        optimizerConfigurations.queryMergerRule = queryMergerRule;
        auto queryMergerPhase = Optimizer::QueryMergerPhase::create(this->z3Context, optimizerConfigurations);
        typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, std::move(udfCatalog));
        auto queryRewritePhase = Optimizer::QueryRewritePhase::create(coordinatorConfiguration);
        auto topologySpecificQueryRewritePhase =
            Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, optimizerConfigurations);
        auto signatureInferencePhase =
            Optimizer::SignatureInferencePhase::create(this->z3Context, optimizerConfigurations.queryMergerRule);

        auto syntacticQueryValidation = Optimizer::SyntacticQueryValidation::create(queryParsingService);
        auto semanticQueryValidation =
            Optimizer::SemanticQueryValidation::create(sourceCatalog,
                                                       udfCatalog,
                                                       coordinatorConfiguration->optimizer.performAdvanceSemanticValidation);

        setupPhysicalSources(sourceCatalog);

        uint64_t totalOperators = 0;
        std::vector<QueryId> queryIds = {};

        auto startTime =
            std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

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
            queryId = PlanIdGenerator::getNextQueryId();
            queryIds.push_back(queryId);
            queryPlan->setQueryId(queryId);
            queryPlan->setPlacementStrategy(queryPlacementStrategy);

            // Perform semantic validation
            semanticQueryValidation->validate(queryPlan);

            //1. Create a new entry in the query catalog
            queryCatalogService->createNewEntry(queryString, queryPlan, queryPlacementStrategy);

            //2. Set query status as Optimizing
            queryCatalogService->updateQueryStatus(queryId, QueryState::OPTIMIZING, "");

            //3. Execute type inference phase
            NES_DEBUG("Performing Query type inference phase for query:  {}", queryId);
            queryPlan = typeInferencePhase->execute(queryPlan);

            //4. Perform query re-write
            NES_DEBUG("Performing Query rewrite phase for query:  {}", queryId);
            queryPlan = queryRewritePhase->execute(queryPlan);

            //6. Execute type inference phase on rewritten query plan
            queryPlan = typeInferencePhase->execute(queryPlan);//TODO: ?

            //7. Perform signature inference phase for sharing identification among query plans
            signatureInferencePhase->execute(queryPlan);

            //8. Perform topology specific rewrites to the query plan
            queryPlan = topologySpecificQueryRewritePhase->execute(queryPlan);

            //10. Perform type inference over re-written query plan
            queryPlan = typeInferencePhase->execute(queryPlan);//TODO: ?

            //compute totalOperators before merge
            totalOperators = totalOperators + PlanIterator(queryPlan).snapshot().size();

            //16. Add the updated query plan to the global query plan
            NES_DEBUG("Performing Query type inference phase for query:  {}", queryId);
            globalQueryPlan->addQueryPlan(queryPlan);
        }//end of for-loop

        //17. Perform query merging for newly added query plan
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

        // NES_INFO("Total operators" + std::to_string(totalOperators) + " , merged operators" + std::to_string(mergedOperators));

        //Compute efficiency
        float efficiency = (((float) totalOperators - (float) mergedOperators) / (float) totalOperators) * 100.0;

        //Compute optimizationTime
        auto optimizationTime = endTime - startTime;

        //get Response as Json
        auto response = getResAsJson(allSQP, efficiency, optimizationTime);

        // respond to the calling service with the needed data
        responsePromise.set_value(std::make_shared<BenchmarkQueryResponse>(response));

        //clear old queries after one query-merging run
        queryCatalogService->clearQueries();
        globalQueryPlan->reset();

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
    for (auto& item : allSQP) {
        auto sharedQueryPlanJson = PlanJsonGenerator::getSharedQueryPlanAsJson(item);
        sharedQueryPlans.push_back(sharedQueryPlanJson);
    }

    resJson["sharedQueryPlans"] = mergeSharedQueryPlanJson(sharedQueryPlans);
    resJson["sharingEfficiency"] = efficiency;
    resJson["optimizationTime"] = optTime;

    return resJson;
}

}// namespace NES::RequestProcessor
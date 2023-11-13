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
#include <Operators/LogicalOperators/UDFs/JavaUDFDescriptor.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Exceptions/ExecutionNodeNotFoundException.hpp>
#include <Optimizer/Exceptions/GlobalQueryPlanUpdateException.hpp>
#include <Operators/Exceptions/InvalidLogicalOperatorException.hpp>
#include <Catalogs/Exceptions/InvalidQueryStateException.hpp>
#include <Catalogs/Exceptions/LogicalSourceNotFoundException.hpp>
#include <Exceptions/MapEntryNotFoundException.hpp>
#include <Optimizer/Exceptions/OperatorNotFoundException.hpp>
#include <Catalogs/Exceptions/PhysicalSourceNotFoundException.hpp>
#include <Exceptions/QueryDeploymentException.hpp>
#include <Catalogs/Exceptions/QueryNotFoundException.hpp>
#include <Optimizer//Exceptions/QueryPlacementException.hpp>
#include <Optimizer/Exceptions/SharedQueryPlanNotFoundException.hpp>
#include <Operators/Exceptions/SignatureComputationException.hpp>
#include <Operators/Exceptions/TypeInferenceException.hpp>
#include <Operators/Exceptions/UDFException.hpp>
#include <Operators/LogicalOperators/OpenCLLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Network/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Network/NetworkSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/Phases/MemoryLayoutSelectionPhase.hpp>
#include <Optimizer/Phases/OriginIdInferencePhase.hpp>
#include <Phases/QueryDeploymentPhase.hpp>
#include <Optimizer/Phases/QueryMergerPhase.hpp>
#include <Optimizer/Phases/QueryPlacementPhase.hpp>
#include <Optimizer/Phases/QueryRewritePhase.hpp>
#include <Phases/QueryUndeploymentPhase.hpp>
#include <Phases/SampleCodeGenerationPhase.hpp>
#include <Optimizer/Phases/SignatureInferencePhase.hpp>
#include <Optimizer/Phases/TopologySpecificQueryRewritePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <QueryValidation/SemanticQueryValidation.hpp>
#include <QueryValidation/SyntacticQueryValidation.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Utils/PlanIdGenerator.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <RequestProcessor/RequestTypes/ExplainRequest.hpp>
#include <RequestProcessor/StorageHandles/ResourceType.hpp>
#include <RequestProcessor/StorageHandles/StorageHandler.hpp>
#include <Catalogs/Query/QueryCatalogService.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/PlacementStrategy.hpp>
#include <cpr/cpr.h>
#include <string>
#include <utility>

namespace NES::RequestProcessor::Experimental {

ExplainRequest::ExplainRequest(const QueryPlanPtr& queryPlan,
                               const Optimizer::PlacementStrategy queryPlacementStrategy,
                               const uint8_t maxRetries,
                               const z3::ContextPtr& z3Context)
    : AbstractRequest({ResourceType::QueryCatalogService,
                       ResourceType::GlobalExecutionPlan,
                       ResourceType::Topology,
                       ResourceType::GlobalQueryPlan,
                       ResourceType::UdfCatalog,
                       ResourceType::SourceCatalog,
                       ResourceType::CoordinatorConfiguration},
                      maxRetries),
      queryId(INVALID_QUERY_ID), queryString(""), queryPlan(queryPlan), queryPlacementStrategy(queryPlacementStrategy),
      z3Context(z3Context), queryParsingService(nullptr) {}

ExplainRequestPtr ExplainRequest::create(const QueryPlanPtr& queryPlan,
                                         const Optimizer::PlacementStrategy queryPlacementStrategy,
                                         const uint8_t maxRetries,
                                         const z3::ContextPtr& z3Context) {
    return std::make_shared<ExplainRequest>(queryPlan, queryPlacementStrategy, maxRetries, z3Context);
}

void ExplainRequest::preRollbackHandle([[maybe_unused]] std::exception_ptr ex,
                                       [[maybe_unused]] const StorageHandlerPtr& storageHandler) {}

std::vector<AbstractRequestPtr> ExplainRequest::rollBack([[maybe_unused]] std::exception_ptr ex,
                                                         [[maybe_unused]] const StorageHandlerPtr& storageHandler) {
    std::rethrow_exception(ex);
}

void ExplainRequest::postRollbackHandle([[maybe_unused]] std::exception_ptr ex,
                                        [[maybe_unused]] const StorageHandlerPtr& storageHandler) {}

void ExplainRequest::postExecution([[maybe_unused]] const StorageHandlerPtr& storageHandler) {}

std::vector<AbstractRequestPtr> ExplainRequest::executeRequestLogic(const StorageHandlerPtr& storageHandler) {
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
        auto queryMergerPhase = Optimizer::QueryMergerPhase::create(this->z3Context, optimizerConfigurations);
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
                                                       udfCatalog,
                                                       coordinatorConfiguration->optimizer.performAdvanceSemanticValidation);

        // assign unique operator identifier to the operators in the query plan
        assignOperatorIds(queryPlan);
        queryString = queryPlan->toString();

        // Set unique identifier and additional properties to the query
        queryId = PlanIdGenerator::getNextQueryId();
        queryPlan->setQueryId(queryId);
        queryPlan->setPlacementStrategy(queryPlacementStrategy);

        // Perform semantic validation
        semanticQueryValidation->validate(queryPlan);

        // respond to the calling service with the query id
        responsePromise.set_value(std::make_shared<ExplainResponse>(queryId));

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
        queryPlan = sampleCodeGenerationPhase->execute(queryPlan);

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

        //21. Perform placement of updated shared query plan
        NES_DEBUG("Performing Operator placement for shared query plan");
        if (!queryPlacementPhase->execute(sharedQueryPlan)) {
            throw Exceptions::QueryPlacementException(sharedQueryId,
                                                      "QueryProcessingService: Failed to perform query placement for "
                                                      "query plan with shared query id: "
                                                          + std::to_string(sharedQueryId));
        }

        //22. Fetch configurations for elegant optimizations
        auto accelerateJavaUdFs = coordinatorConfiguration->elegant.accelerateJavaUDFs;
        auto accelerationServiceURL = coordinatorConfiguration->elegant.accelerationServiceURL;

        //23. Compute the json response explaining the optimization steps
        auto response = getExecutionPlanForSharedQueryAsJson(sharedQueryId,
                                                             globalExecutionPlan,
                                                             topology,
                                                             accelerateJavaUdFs,
                                                             accelerationServiceURL,
                                                             sampleCodeGenerationPhase);

        //24. respond to the calling service with the query id
        responsePromise.set_value(std::make_shared<ExplainResponse>(response));

        //25. clean up the data structure
        globalQueryPlan->removeQuery(queryId, RequestType::StopQuery);
        globalExecutionPlan->removeQuerySubPlans(queryId);

        //26. Set query status as Explained
        queryCatalogService->updateQueryStatus(queryId, QueryState::EXPLAINED, "");
    } catch (RequestExecutionException& exception) {
        NES_ERROR("Exception occurred while processing ExplainRequest with error {}", exception.what());
        handleError(std::current_exception(), storageHandler);
    }
    return {};
}

void ExplainRequest::assignOperatorIds(const QueryPlanPtr& queryPlan) {
    // Iterate over all operators in the query and replace the client-provided ID
    auto queryPlanIterator = QueryPlanIterator(queryPlan);
    for (auto itr = queryPlanIterator.begin(); itr != QueryPlanIterator::end(); ++itr) {
        auto visitingOp = (*itr)->as<OperatorNode>();
        visitingOp->setId(NES::getNextOperatorId());
    }
}

nlohmann::json
ExplainRequest::getExecutionPlanForSharedQueryAsJson(SharedQueryId sharedQueryId,
                                                     const GlobalExecutionPlanPtr& globalExecutionPlan,
                                                     const TopologyPtr& topology,
                                                     bool accelerateJavaUDFs,
                                                     const std::string& accelerationServiceURL,
                                                     const Optimizer::SampleCodeGenerationPhasePtr& sampleCodeGenerationPhase) {
    NES_INFO("UtilityFunctions: getting execution plan as JSON");

    nlohmann::json executionPlanJson{};
    std::vector<nlohmann::json> nodes = {};

    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(sharedQueryId);
    for (const ExecutionNodePtr& executionNode : executionNodes) {
        nlohmann::json executionNodeMetaData{};

        executionNodeMetaData["nodeId"] = executionNode->getId();
        auto topologyNode = topology->findNodeWithId(executionNode->getId());

        // loop over all query sub plans inside the current executionNode
        nlohmann::json scheduledSubQueries{};
        for (const auto& querySubPlan : executionNode->getQuerySubPlans(sharedQueryId)) {

            // prepare json object to hold information on current query sub plan
            nlohmann::json currentQuerySubPlanMetaData{};

            // id of current query sub plan
            currentQuerySubPlanMetaData["querySubPlanId"] = querySubPlan->getQuerySubPlanId();

            // add open cl acceleration code
            if (accelerateJavaUDFs) {
                addOpenCLAccelerationCode(accelerationServiceURL, querySubPlan, topologyNode);
            }

            // add logical query plan to the json object
            currentQuerySubPlanMetaData["logicalQuerySubPlan"] = querySubPlan->toString();

            // Generate Code Snippets for the sub query plan
            auto updatedSubQueryPlan = sampleCodeGenerationPhase->execute(querySubPlan);
            std::vector<std::string> generatedCodeSnippets;
            std::set<uint64_t> pipelineIds;
            auto queryPlanIterator = QueryPlanIterator(updatedSubQueryPlan);
            for (auto itr = queryPlanIterator.begin(); itr != QueryPlanIterator::end(); ++itr) {
                auto visitingOp = (*itr)->as<OperatorNode>();
                auto pipelineId = std::any_cast<uint64_t>(visitingOp->getProperty("PIPELINE_ID"));
                if (pipelineIds.emplace(pipelineId).second) {
                    generatedCodeSnippets.emplace_back(std::any_cast<std::string>(visitingOp->getProperty("SOURCE_CODE")));
                }
            }

            // Added generated code snippets to the response
            nlohmann::json generatedPipelineCodeSnippets{};
            for (const auto& generatedCodeSnippet : generatedCodeSnippets){
                generatedPipelineCodeSnippets.emplace_back(generatedCodeSnippet);
            }

            currentQuerySubPlanMetaData["Pipelines"] = generatedPipelineCodeSnippets;

            scheduledSubQueries.push_back(currentQuerySubPlanMetaData);
        }
        executionNodeMetaData["querySubPlans"] = scheduledSubQueries;
        nodes.push_back(executionNodeMetaData);
    }

    // add `executionNodes` JSON array to the final JSON result
    executionPlanJson["executionNodes"] = nodes;

    return executionPlanJson;
}

void ExplainRequest::addOpenCLAccelerationCode(const std::string& accelerationServiceURL,
                                               const QueryPlanPtr& queryPlan,
                                               const TopologyNodePtr& topologyNode) {

    //Elegant acceleration service call
    //1. Fetch the OpenCL Operators
    auto openCLOperators = queryPlan->getOperatorByType<OpenCLLogicalOperatorNode>();

    //2. Iterate over all open CL operators and set the Open CL code returned by the acceleration service
    for (const auto& openCLOperator : openCLOperators) {

        // FIXME: populate information from node with the correct property keys
        // FIXME: pick a naming + id scheme for deviceName
        //3. Fetch the topology node and compute the topology node payload
        nlohmann::json payload;
        nlohmann::json deviceInfo;
        deviceInfo[DEVICE_INFO_NAME_KEY] = std::any_cast<std::string>(topologyNode->getNodeProperty("DEVICE_NAME"));
        deviceInfo[DEVICE_INFO_DOUBLE_FP_SUPPORT_KEY] =
            std::any_cast<bool>(topologyNode->getNodeProperty("DEVICE_DOUBLE_FP_SUPPORT"));
        nlohmann::json maxWorkItems{};
        maxWorkItems[DEVICE_MAX_WORK_ITEMS_DIM1_KEY] =
            std::any_cast<uint64_t>(topologyNode->getNodeProperty("DEVICE_MAX_WORK_ITEMS_DIM1"));
        maxWorkItems[DEVICE_MAX_WORK_ITEMS_DIM2_KEY] =
            std::any_cast<uint64_t>(topologyNode->getNodeProperty("DEVICE_MAX_WORK_ITEMS_DIM2"));
        maxWorkItems[DEVICE_MAX_WORK_ITEMS_DIM3_KEY] =
            std::any_cast<uint64_t>(topologyNode->getNodeProperty("DEVICE_MAX_WORK_ITEMS_DIM3"));
        deviceInfo[DEVICE_MAX_WORK_ITEMS_KEY] = maxWorkItems;
        deviceInfo[DEVICE_INFO_ADDRESS_BITS_KEY] =
            std::any_cast<std::string>(topologyNode->getNodeProperty("DEVICE_ADDRESS_BITS"));
        deviceInfo[DEVICE_INFO_EXTENSIONS_KEY] = std::any_cast<std::string>(topologyNode->getNodeProperty("DEVICE_EXTENSIONS"));
        deviceInfo[DEVICE_INFO_AVAILABLE_PROCESSORS_KEY] =
            std::any_cast<uint64_t>(topologyNode->getNodeProperty("DEVICE_AVAILABLE_PROCESSORS"));
        payload[DEVICE_INFO_KEY] = deviceInfo;

        //4. Extract the Java UDF metadata
        auto javaDescriptor = openCLOperator->getJavaUDFDescriptor();
        payload["functionCode"] = javaDescriptor->getMethodName();

        //find the bytecode for the udf class
        auto className = javaDescriptor->getClassName();
        auto byteCodeList = javaDescriptor->getByteCodeList();
        auto classByteCode = std::find_if(byteCodeList.cbegin(), byteCodeList.cend(), [&](const jni::JavaClassDefinition& c) {
            return c.first == className;
        });

        if (classByteCode == byteCodeList.end()) {
            throw QueryDeploymentException(queryId,
                                           "The bytecode list of classes implementing the "
                                           "UDF must contain the fully-qualified name of the UDF");
        }
        jni::JavaByteCode javaByteCode = classByteCode->second;

        //5. Prepare the multi-part message
        cpr::Part part1 = {"firstPayload", to_string(payload)};
        cpr::Part part2 = {"secondPayload", &javaByteCode[0]};
        cpr::Multipart multipartPayload = cpr::Multipart{part1, part2};

        //6. Make Acceleration Service Call
        cpr::Response response = cpr::Post(cpr::Url{accelerationServiceURL},
                                           cpr::Header{{"Content-Type", "application/json"}},
                                           multipartPayload,
                                           cpr::Timeout(ELEGANT_SERVICE_TIMEOUT));
        if (response.status_code != 200) {
            throw QueryDeploymentException(queryPlan->getQueryId(),
                                           "ExplainRequest: Error in call to Elegant acceleration service with code "
                                               + std::to_string(response.status_code) + " and msg " + response.reason);
        }

        nlohmann::json jsonResponse = nlohmann::json::parse(response.text);
        //Fetch the acceleration Code
        //FIXME: use the correct key
        auto openCLCode = jsonResponse["AccelerationCode"];
        //6. Set the Open CL code
        openCLOperator->setOpenClCode(openCLCode);
    }
}

}// namespace NES::RequestProcessor::Experimental

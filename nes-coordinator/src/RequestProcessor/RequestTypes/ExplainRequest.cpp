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
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/WorkerConfigurationKeys.hpp>
#include <Exceptions/MapEntryNotFoundException.hpp>
#include <Exceptions/QueryDeploymentException.hpp>
#include <Identifiers/NESStrongTypeJson.hpp>
#include <Operators/LogicalOperators/LogicalOpenCLOperator.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperator.hpp>
#include <Operators/LogicalOperators/UDFs/JavaUDFDescriptor.hpp>
#include <Optimizer/Exceptions/SharedQueryPlanNotFoundException.hpp>
#include <Optimizer/Phases/MemoryLayoutSelectionPhase.hpp>
#include <Optimizer/Phases/OriginIdInferencePhase.hpp>
#include <Optimizer/Phases/QueryMergerPhase.hpp>
#include <Optimizer/Phases/QueryPlacementAmendmentPhase.hpp>
#include <Optimizer/Phases/QueryRewritePhase.hpp>
#include <Optimizer/Phases/SignatureInferencePhase.hpp>
#include <Optimizer/Phases/TopologySpecificQueryRewritePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryPlacementAddition/ElegantPlacementStrategy.hpp>
#include <Phases/SampleCodeGenerationPhase.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Utils/PlanIdGenerator.hpp>
#include <Plans/Utils/PlanIterator.hpp>
#include <QueryValidation/SemanticQueryValidation.hpp>
#include <QueryValidation/SyntacticQueryValidation.hpp>
#include <RequestProcessor/RequestTypes/ExplainRequest.hpp>
#include <RequestProcessor/StorageHandles/ResourceType.hpp>
#include <RequestProcessor/StorageHandles/StorageHandler.hpp>
#include <Runtime/OpenCLDeviceInfo.hpp>
#include <Util/DeploymentContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Placement/PlacementStrategy.hpp>
#include <cpr/cpr.h>
#include <string>
#include <utility>
#include <Util/CheckpointStorageType.hpp>

namespace NES::RequestProcessor {

ExplainRequest::ExplainRequest(const QueryPlanPtr& queryPlan,
                               const Optimizer::PlacementStrategy queryPlacementStrategy,
                               FaultToleranceType faultToleranceType,
                               CheckpointStorageType checkpointStorage,
                               const uint8_t maxRetries,
                               const z3::ContextPtr& z3Context)
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
      z3Context(z3Context), queryParsingService(nullptr), faultToleranceType(faultToleranceType),
      checkpointStorage(checkpointStorage) {}

ExplainRequestPtr ExplainRequest::create(const QueryPlanPtr& queryPlan,
                                         const Optimizer::PlacementStrategy queryPlacementStrategy,
                                         FaultToleranceType faultToleranceType,
                                         CheckpointStorageType checkpointStorage,
                                         const uint8_t maxRetries,
                                         const z3::ContextPtr& z3Context) {
    return std::make_shared<ExplainRequest>(queryPlan, queryPlacementStrategy, faultToleranceType, checkpointStorage, maxRetries, z3Context);
}

void ExplainRequest::preRollbackHandle([[maybe_unused]] std::exception_ptr ex,
                                       [[maybe_unused]] const StorageHandlerPtr& storageHandler) {}

std::vector<AbstractRequestPtr> ExplainRequest::rollBack([[maybe_unused]] std::exception_ptr ex,
                                                         [[maybe_unused]] const StorageHandlerPtr& storageHandler) {
    std::rethrow_exception(ex);
}

void ExplainRequest::postRollbackHandle([[maybe_unused]] std::exception_ptr ex,
                                        [[maybe_unused]] const StorageHandlerPtr& storageHandler) {}

std::vector<AbstractRequestPtr> ExplainRequest::executeRequestLogic(const StorageHandlerPtr& storageHandler) {
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

        // assign unique operator identifier to the operators in the query plan
        assignOperatorIds(queryPlan);
        queryString = queryPlan->toString();

        // Set unique identifier and additional properties to the query
        queryId = PlanIdGenerator::getNextQueryId();
        queryPlan->setQueryId(queryId);
        queryPlan->setPlacementStrategy(queryPlacementStrategy);
        queryPlan->setFaultTolerance(faultToleranceType);
        queryPlan->setCheckpointStorage(checkpointStorage);

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
        queryPlan = typeInferencePhase->execute(queryPlan, faultToleranceType);

        //4. Set memory layout of each logical operator
        NES_DEBUG("Performing query choose memory layout phase:  {}", queryId);
        queryPlan = memoryLayoutSelectionPhase->execute(queryPlan);

        //5. Perform query re-write
        NES_DEBUG("Performing Query rewrite phase for query:  {}", queryId);
        queryPlan = queryRewritePhase->execute(queryPlan);

        //6. Add the updated query plan to the query catalog
        queryCatalog->addUpdatedQueryPlan(queryId, "Query Rewrite Phase", queryPlan);

        //7. Execute type inference phase on rewritten query plan
        queryPlan = typeInferencePhase->execute(queryPlan, faultToleranceType);

        //8. Generate sample code for elegant planner
        queryPlan = sampleCodeGenerationPhase->execute(queryPlan);

        //9. Perform signature inference phase for sharing identification among query plans
        signatureInferencePhase->execute(queryPlan);

        //10. Perform topology specific rewrites to the query plan
        queryPlan = topologySpecificQueryRewritePhase->execute(queryPlan);

        //11. Add the updated query plan to the query catalog
        queryCatalog->addUpdatedQueryPlan(queryId, "Topology Specific Query Rewrite Phase", queryPlan);

        //12. Perform type inference over re-written query plan
        queryPlan = typeInferencePhase->execute(queryPlan, faultToleranceType);

        //13. Identify the number of origins and their ids for all logical operators
        queryPlan = originIdInferencePhase->execute(queryPlan);

        //14. Set memory layout of each logical operator in the rewritten query
        NES_DEBUG("Performing query choose memory layout phase:  {}", queryId);
        queryPlan = memoryLayoutSelectionPhase->execute(queryPlan);

        //15. Add the updated query plan to the query catalog
        queryCatalog->addUpdatedQueryPlan(queryId, "Executed Query Plan", queryPlan);

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
        queryPlacementAmendmentPhase->execute(sharedQueryPlan);

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

        //24. clean up the global query plan by marking it for removal
        globalQueryPlan->removeQuery(queryId, RequestType::StopQuery);
        //25. Get the shared query plan for the added query
        sharedQueryPlan = globalQueryPlan->getSharedQueryPlan(sharedQueryId);
        //26. Perform placement removal of updated shared query plan
        NES_DEBUG("Performing Operator placement for shared query plan");
        auto deploymentUnit = queryPlacementAmendmentPhase->execute(sharedQueryPlan);

        //27. Set query status as Explained
        queryCatalog->updateQueryStatus(queryId, QueryState::EXPLAINED, "");

        //28. Iterate over deployment context and update execution plan
        for (const auto& deploymentContext : deploymentUnit.getAllDeploymentContexts()) {
            auto WorkerId = deploymentContext->getWorkerId();
            auto decomposedQueryId = deploymentContext->getDecomposedQueryId();
            auto decomposedQueryPlanVersion = deploymentContext->getDecomposedQueryPlanVersion();
            auto decomposedQueryPlanState = deploymentContext->getDecomposedQueryPlanState();
            switch (decomposedQueryPlanState) {
                case QueryState::MARKED_FOR_REDEPLOYMENT:
                case QueryState::MARKED_FOR_DEPLOYMENT: {
                    globalExecutionPlan->updateDecomposedQueryPlanState(WorkerId,
                                                                        sharedQueryId,
                                                                        decomposedQueryId,
                                                                        decomposedQueryPlanVersion,
                                                                        QueryState::RUNNING);
                    break;
                }
                case QueryState::MARKED_FOR_MIGRATION: {
                    globalExecutionPlan->updateDecomposedQueryPlanState(WorkerId,
                                                                        sharedQueryId,
                                                                        decomposedQueryId,
                                                                        decomposedQueryPlanVersion,
                                                                        QueryState::STOPPED);
                    globalExecutionPlan->removeDecomposedQueryPlan(WorkerId,
                                                                   sharedQueryId,
                                                                   decomposedQueryId,
                                                                   decomposedQueryPlanVersion);
                    break;
                }
                default:
                    NES_WARNING("Unhandled Deployment context with status: {}", magic_enum::enum_name(decomposedQueryPlanState));
            }
        }

        //29. respond to the calling service with the query id
        responsePromise.set_value(std::make_shared<ExplainResponse>(response));
    } catch (RequestExecutionException& exception) {
        NES_ERROR("Exception occurred while processing ExplainRequest with error {}", exception.what());
        responsePromise.set_value(std::make_shared<ExplainResponse>(""));
        handleError(std::current_exception(), storageHandler);
    }
    return {};
}

void ExplainRequest::assignOperatorIds(const QueryPlanPtr& queryPlan) {
    // Iterate over all operators in the query and replace the client-provided ID
    auto queryPlanIterator = PlanIterator(queryPlan);
    for (auto itr = queryPlanIterator.begin(); itr != PlanIterator::end(); ++itr) {
        auto visitingOp = (*itr)->as<Operator>();
        visitingOp->setId(NES::getNextOperatorId());
    }
}

nlohmann::json
ExplainRequest::getExecutionPlanForSharedQueryAsJson(SharedQueryId sharedQueryId,
                                                     const Optimizer::GlobalExecutionPlanPtr& globalExecutionPlan,
                                                     const TopologyPtr& topology,
                                                     bool accelerateJavaUDFs,
                                                     const std::string& accelerationServiceURL,
                                                     const Optimizer::SampleCodeGenerationPhasePtr& sampleCodeGenerationPhase) {
    NES_INFO("UtilityFunctions: getting execution plan as JSON");

    nlohmann::json executionPlanJson{};
    std::vector<nlohmann::json> nodes = {};

    auto lockedExecutionNodes = globalExecutionPlan->getLockedExecutionNodesHostingSharedQueryId(sharedQueryId);
    for (const auto& lockedExecutionNode : lockedExecutionNodes) {
        nlohmann::json executionNodeMetaData{};

        executionNodeMetaData["nodeId"] = lockedExecutionNode->operator*()->getId();
        auto topologyNode = topology->getCopyOfTopologyNodeWithId(lockedExecutionNode->operator*()->getId());

        // loop over all query sub plans inside the current executionNode
        nlohmann::json scheduledSubQueries{};
        for (const auto& decomposedQueryPlan : lockedExecutionNode->operator*()->getAllDecomposedQueryPlans(sharedQueryId)) {

            // prepare json object to hold information on current query sub plan
            nlohmann::json currentQuerySubPlanMetaData{};

            // id of current query sub plan
            currentQuerySubPlanMetaData["querySubPlanId"] = decomposedQueryPlan->getDecomposedQueryId();

            // add open cl acceleration code
            if (accelerateJavaUDFs) {
                addOpenCLAccelerationCode(accelerationServiceURL, decomposedQueryPlan, topologyNode);
            }

            // add logical query plan to the json object
            currentQuerySubPlanMetaData["logicalQuerySubPlan"] = decomposedQueryPlan->toString();

            // Generate Code Snippets for the sub query plan
            auto updatedSubQueryPlan = sampleCodeGenerationPhase->execute(decomposedQueryPlan);
            std::vector<std::string> generatedCodeSnippets;
            std::set<PipelineId> pipelineIds;
            auto queryPlanIterator = PlanIterator(updatedSubQueryPlan);
            for (auto itr = queryPlanIterator.begin(); itr != PlanIterator::end(); ++itr) {
                auto visitingOp = (*itr)->as<Operator>();
                if (visitingOp->hasProperty("PIPELINE_ID")) {
                    auto pipelineId = std::any_cast<PipelineId>(visitingOp->getProperty("PIPELINE_ID"));
                    if (pipelineIds.emplace(pipelineId).second) {
                        generatedCodeSnippets.emplace_back(std::any_cast<std::string>(
                            visitingOp->getProperty(Optimizer::ElegantPlacementStrategy::sourceCodeKey)));
                    }
                }
            }

            // Added generated code snippets to the response
            currentQuerySubPlanMetaData["Pipelines"] = generatedCodeSnippets;

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
                                               const DecomposedQueryPlanPtr& decomposedQueryPlan,
                                               const TopologyNodePtr& topologyNode) {

    //Elegant acceleration service call
    //1. Fetch the OpenCL Operators
    auto openCLOperators = decomposedQueryPlan->getOperatorByType<LogicalOpenCLOperator>();

    //2. Iterate over all open CL operators and set the Open CL code returned by the acceleration service
    for (const auto& openCLOperator : openCLOperators) {
        //3. Fetch the topology node and compute the topology node payload
        nlohmann::json payload;
        payload[DEVICE_INFO_KEY] = std::any_cast<std::vector<NES::Runtime::OpenCLDeviceInfo>>(
            topologyNode->getNodeProperty(NES::Worker::Configuration::OPENCL_DEVICES))[openCLOperator->getDeviceId()];

        //4. Extract the Java UDF metadata
        auto javaDescriptor = openCLOperator->getJavaUDFDescriptor();
        payload["functionCode"] = javaDescriptor->getMethodName();

        // The constructor of the Java UDF descriptor ensures that the byte code of the class exists.
        jni::JavaByteCode javaByteCode = javaDescriptor->getClassByteCode(javaDescriptor->getClassName()).value();

        //5. Prepare the multi-part message
        cpr::Part part1 = {"jsonFile", to_string(payload)};
        cpr::Part part2 = {"codeFile", &javaByteCode[0]};
        cpr::Multipart multipartPayload = cpr::Multipart{part1, part2};

        //6. Make Acceleration Service Call
        cpr::Response response = cpr::Post(cpr::Url{accelerationServiceURL},
                                           cpr::Header{{"Content-Type", "application/json"}},
                                           multipartPayload,
                                           cpr::Timeout(ELEGANT_SERVICE_TIMEOUT));
        if (response.status_code != 200) {
            throw QueryDeploymentException(
                UNSURE_CONVERSION_TODO_4761(decomposedQueryPlan->getDecomposedQueryId(), SharedQueryId),
                "Error in call to Elegant acceleration service with code " + std::to_string(response.status_code) + " and msg "
                    + response.reason);
        }

        nlohmann::json jsonResponse = nlohmann::json::parse(response.text);
        //Fetch the acceleration Code
        //FIXME: use the correct key
        auto openCLCode = jsonResponse["AccelerationCode"];
        //6. Set the Open CL code
        openCLOperator->setOpenClCode(openCLCode);
    }
}

}// namespace NES::RequestProcessor

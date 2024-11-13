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
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/UDF/UDFCatalog.hpp>
#include <Configurations/Coordinator/OptimizerConfiguration.hpp>
#include <Operators/LogicalOperators/StatisticCollection/SendingPolicy/SendingPolicyASAP.hpp>
#include <Operators/LogicalOperators/StatisticCollection/TriggerCondition/NeverTrigger.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/PlanIdGenerator.hpp>
#include <Plans/Utils/PlanIterator.hpp>
#include <QueryValidation/SemanticQueryValidation.hpp>
#include <QueryValidation/SyntacticQueryValidation.hpp>
#include <RequestProcessor/AsyncRequestProcessor.hpp>
#include <RequestProcessor/RequestTypes/AddQueryRequest.hpp>
#include <RequestProcessor/RequestTypes/ExplainRequest.hpp>
#include <RequestProcessor/RequestTypes/FailQueryRequest.hpp>
#include <RequestProcessor/RequestTypes/ISQP/ISQPRequest.hpp>
#include <RequestProcessor/RequestTypes/SourceCatalog/GetSourceCatalogRequest.hpp>
#include <RequestProcessor/RequestTypes/SourceCatalog/SourceCatalogEvents/AddKeyDistributionEntryEvent.hpp>
#include <RequestProcessor/RequestTypes/SourceCatalog/SourceCatalogEvents/AddLogicalSourceEvent.hpp>
#include <RequestProcessor/RequestTypes/SourceCatalog/SourceCatalogEvents/AddPhysicalSourcesEvent.hpp>
#include <RequestProcessor/RequestTypes/SourceCatalog/SourceCatalogEvents/GetAllLogicalSourcesEvent.hpp>
#include <RequestProcessor/RequestTypes/SourceCatalog/SourceCatalogEvents/GetPhysicalSourcesEvent.hpp>
#include <RequestProcessor/RequestTypes/SourceCatalog/SourceCatalogEvents/GetSchemaEvent.hpp>
#include <RequestProcessor/RequestTypes/SourceCatalog/SourceCatalogEvents/RemoveAllPhysicalSourcesByWorkerEvent.hpp>
#include <RequestProcessor/RequestTypes/SourceCatalog/SourceCatalogEvents/RemoveLogicalSourceEvent.hpp>
#include <RequestProcessor/RequestTypes/SourceCatalog/SourceCatalogEvents/RemovePhysicalSourceEvent.hpp>
#include <RequestProcessor/RequestTypes/SourceCatalog/SourceCatalogEvents/UpdateLogicalSourceEvent.hpp>
#include <RequestProcessor/RequestTypes/SourceCatalog/UpdateSourceCatalogRequest.hpp>
#include <RequestProcessor/RequestTypes/StopQueryRequest.hpp>
#include <Services/RequestHandlerService.hpp>
#include <StatisticCollection/QueryGeneration/AbstractStatisticQueryGenerator.hpp>
#include <StatisticCollection/StatisticRegistry/StatisticInfo.hpp>
#include <StatisticCollection/StatisticRegistry/StatisticRegistry.hpp>
#include <Util/Core.hpp>
#include <Util/FaultToleranceType.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Placement/PlacementStrategy.hpp>
#include <llvm/Support/MathExtras.h>
#include <nlohmann/json.hpp>

namespace NES {

RequestHandlerService::RequestHandlerService(Configurations::OptimizerConfiguration optimizerConfiguration,
                                             const QueryParsingServicePtr& queryParsingService,
                                             const Catalogs::Query::QueryCatalogPtr& queryCatalog,
                                             const Catalogs::Source::SourceCatalogPtr& sourceCatalog,
                                             const Catalogs::UDF::UDFCatalogPtr& udfCatalog,
                                             const NES::RequestProcessor::AsyncRequestProcessorPtr& asyncRequestExecutor,
                                             const z3::ContextPtr& z3Context,
                                             const Statistic::AbstractStatisticQueryGeneratorPtr& statisticQueryGenerator,
                                             const Statistic::StatisticRegistryPtr& statisticRegistry,
                                             const Optimizer::PlacementAmendmentHandlerPtr& placementAmendmentHandler)
    : optimizerConfiguration(optimizerConfiguration), queryParsingService(queryParsingService), queryCatalog(queryCatalog),
      asyncRequestExecutor(asyncRequestExecutor), z3Context(z3Context), statisticQueryGenerator(statisticQueryGenerator),
      statisticRegistry(statisticRegistry), placementAmendmentHandler(placementAmendmentHandler) {
    NES_DEBUG("RequestHandlerService()");
    syntacticQueryValidation = Optimizer::SyntacticQueryValidation::create(this->queryParsingService);
    semanticQueryValidation = Optimizer::SemanticQueryValidation::create(sourceCatalog,
                                                                         udfCatalog,
                                                                         optimizerConfiguration.performAdvanceSemanticValidation);
}

QueryId RequestHandlerService::validateAndQueueAddQueryRequest(const std::string& queryString,
                                                               const Optimizer::PlacementStrategy placementStrategy,
                                                               FaultToleranceType faultTolerance) {

    auto addRequest = RequestProcessor::AddQueryRequest::create(queryString,
                                                                placementStrategy,
                                                                RequestProcessor::DEFAULT_RETRIES,
                                                                z3Context,
                                                                queryParsingService,
                                                                placementAmendmentHandler,
                                                                faultTolerance);
    asyncRequestExecutor->runAsync(addRequest);
    auto future = addRequest->getFuture();
    return std::static_pointer_cast<RequestProcessor::AddQueryResponse>(future.get())->queryId;
}

QueryId RequestHandlerService::validateAndQueueAddQueryRequest(const QueryPlanPtr& queryPlan,
const Optimizer::PlacementStrategy placementStrategy,
FaultToleranceType faultTolerance) {

    auto addRequest = RequestProcessor::AddQueryRequest::create(queryPlan,
                                                                placementStrategy,
                                                                RequestProcessor::DEFAULT_RETRIES,
                                                                z3Context,
                                                                placementAmendmentHandler,
                                                                faultTolerance);
    asyncRequestExecutor->runAsync(addRequest);
    auto future = addRequest->getFuture();
    return std::static_pointer_cast<RequestProcessor::AddQueryResponse>(future.get())->queryId;
}

nlohmann::json RequestHandlerService::validateAndQueueExplainQueryRequest(const NES::QueryPlanPtr& queryPlan,
const Optimizer::PlacementStrategy placementStrategy,
FaultToleranceType faultTolerance) {

    auto explainRequest = RequestProcessor::ExplainRequest::create(queryPlan, placementStrategy, faultTolerance, 1, z3Context);
    asyncRequestExecutor->runAsync(explainRequest);
    auto future = explainRequest->getFuture();
    return std::static_pointer_cast<RequestProcessor::ExplainResponse>(future.get())->jsonResponse;
}

bool RequestHandlerService::validateAndQueueStopQueryRequest(QueryId queryId) {

    try {
        auto stopRequest =
            RequestProcessor::StopQueryRequest::create(queryId, RequestProcessor::DEFAULT_RETRIES, placementAmendmentHandler);
        asyncRequestExecutor->runAsync(stopRequest);
        auto future = stopRequest->getFuture();
        auto response = future.get();
        auto success = std::static_pointer_cast<RequestProcessor::StopQueryResponse>(response)->success;
        return success;
    } catch (Exceptions::InvalidQueryStateException& e) {
        //return true if the query was already stopped
        return (e.getActualState() == QueryState::STOPPED || e.getActualState() == QueryState::MARKED_FOR_HARD_STOP);
    }
}

bool RequestHandlerService::validateAndQueueFailQueryRequest(SharedQueryId sharedQueryId,
                                                             DecomposedQueryId decomposedQueryId,
                                                             DecomposedQueryPlanVersion decomposedQueryVersion,
                                                             const std::string& failureReason) {

    auto failRequest = RequestProcessor::FailQueryRequest::create(sharedQueryId,
                                                                  decomposedQueryId,
                                                                  decomposedQueryVersion,
                                                                  failureReason,
                                                                  RequestProcessor::DEFAULT_RETRIES,
                                                                  placementAmendmentHandler);
    asyncRequestExecutor->runAsync(failRequest);
    auto future = failRequest->getFuture();
    auto returnedSharedQueryId = std::static_pointer_cast<RequestProcessor::FailQueryResponse>(future.get())->sharedQueryId;
    return returnedSharedQueryId != INVALID_SHARED_QUERY_ID;
}

void RequestHandlerService::assignOperatorIds(QueryPlanPtr queryPlan) {
    // Iterate over all operators in the query and replace the client-provided ID
    auto queryPlanIterator = PlanIterator(queryPlan);
    for (auto itr = queryPlanIterator.begin(); itr != PlanIterator::end(); ++itr) {
        auto visitingOp = (*itr)->as<Operator>();
        visitingOp->setId(getNextOperatorId());
    }
}

RequestProcessor::ISQPRequestResponsePtr
RequestHandlerService::queueISQPRequest(const std::vector<RequestProcessor::ISQPEventPtr>& isqpEvents) {

    // 1. Compute an ISQP request for the collection of external change events (ISQP events)
    auto isqpRequest = RequestProcessor::ISQPRequest::create(placementAmendmentHandler,
                                                             z3Context,
                                                             isqpEvents,
                                                             RequestProcessor::DEFAULT_RETRIES);

    // 2. Execute the request
    asyncRequestExecutor->runAsync(isqpRequest);

    // 3. Wait for the response
    auto future = isqpRequest->getFuture();
    return std::static_pointer_cast<RequestProcessor::ISQPRequestResponse>(future.get());
}

std::vector<Statistic::StatisticKey>
RequestHandlerService::trackStatisticRequest(const Statistic::CharacteristicPtr& characteristic,
                                             const Windowing::WindowTypePtr& window,
                                             const Statistic::TriggerConditionPtr& triggerCondition,
                                             const Statistic::SendingPolicyPtr& sendingPolicy,
                                             std::function<void(Statistic::CharacteristicPtr)> callBack) {
    using namespace Statistic;

    // 1. Creating a query that collects the required statistic
    const auto statisticQuery =
        statisticQueryGenerator->createStatisticQuery(*characteristic, window, sendingPolicy, triggerCondition, *queryCatalog);

    // 2. Submitting the query to the system
    const auto queryId = validateAndQueueAddQueryRequest(statisticQuery.getQueryPlan(), Optimizer::PlacementStrategy::BottomUp);

    // 3. Extracting all statistic ids from the submitted queries to create StatisticKeys out of the statistic ids
    const auto newStatisticIds = statisticIdsExtractor.extractStatisticIdsFromQueryId(queryCatalog, queryId);
    const auto metric = characteristic->getType();

    // 4. For each statistic id, we create a new StatisticKey and also insert it into the registry, if the StatisticKey
    // is not already contained in the registry
    std::vector<StatisticKey> statisticKeysForThisTrackRequest;
    for (const auto& statisticId : newStatisticIds) {
        StatisticKey newKey(metric, statisticId);
        statisticKeysForThisTrackRequest.emplace_back(newKey);
        if (!statisticRegistry->contains(newKey.hash())) {
            StatisticInfo statisticInfo(window, triggerCondition, callBack, queryId, metric);
            statisticRegistry->insert(newKey.hash(), statisticInfo);
        }
    }

    // 5. We return all statisticKeys that belong to this track request, so that they can be used for probing the statistics
    return statisticKeysForThisTrackRequest;
}

std::vector<Statistic::StatisticKey>
RequestHandlerService::trackStatisticRequest(const Statistic::CharacteristicPtr& characteristic,
                                             const Windowing::WindowTypePtr& window) {
    return trackStatisticRequest(characteristic,
                                 window,
                                 Statistic::NeverTrigger::create(),
                                 Statistic::SENDING_ASAP(Statistic::StatisticDataCodec::DEFAULT),
                                 nullptr);
}

bool RequestHandlerService::queueRegisterPhysicalSourceRequest(std::vector<RequestProcessor::PhysicalSourceDefinition> additions,
                                                               WorkerId workerId) const {
    NES_DEBUG("request to register Physical source");
    auto event = RequestProcessor::AddPhysicalSourcesEvent::create(additions, workerId);
    return handleCatalogUpdateRequest(event);
}

bool RequestHandlerService::queueRegisterLogicalSourceRequest(const std::string& logicalSourceName, SchemaPtr schema) const {
    NES_DEBUG("request to register logical source");
    auto event = RequestProcessor::AddLogicalSourceEvent::create(logicalSourceName, schema);
    return handleCatalogUpdateRequest(event);
}

bool RequestHandlerService::queueUnregisterPhysicalSourceRequest(const std::string& physicalSourceName,
                                                                 const std::string& logicalSourceName,
                                                                 WorkerId workerId) const {
    NES_DEBUG("request to unregister physical source");
    auto event = RequestProcessor::RemovePhysicalSourceEvent::create(logicalSourceName, physicalSourceName, workerId);
    return handleCatalogUpdateRequest(event);
}

bool RequestHandlerService::queueUnregisterAllPhysicalSourcesByWorkerRequest(WorkerId workerId) const {
    NES_DEBUG("SourceCatalogService::unregisterAllPhysicalSourcesByWorker: remove all physical sources for worker: {}", workerId);
    auto event = RequestProcessor::RemoveAllPhysicalSourcesByWorkerEvent::create(workerId);
    return handleCatalogUpdateRequest(event);
}

bool RequestHandlerService::queueAddKeyDistributionEntryRequest(const std::string& logicalSourceName,
                                                                const std::string& physicalSourceName,
                                                                WorkerId workerId,
                                                                const std::string& value) const {
    NES_DEBUG("request to add key distribution entry");
    auto event = RequestProcessor::AddKeyDistributionEntryEvent::create(logicalSourceName, physicalSourceName, workerId, value);
    return handleCatalogUpdateRequest(event);
}

bool RequestHandlerService::queueUnregisterLogicalSourceRequest(const std::string& logicalSourceName) const {
    NES_DEBUG("request to unregister logical source");
    auto event = RequestProcessor::RemoveLogicalSourceEvent::create(logicalSourceName);
    return handleCatalogUpdateRequest(event);
}

bool RequestHandlerService::queueUpdateLogicalSourceRequest(const std::string& logicalSourceName, SchemaPtr schema) const {
    NES_DEBUG("request to update logical source");
    auto event = RequestProcessor::UpdateLogicalSourceEvent::create(logicalSourceName, schema);
    return handleCatalogUpdateRequest(event);
}

nlohmann::json RequestHandlerService::queueGetAllLogicalSourcesRequest() const {
    NES_DEBUG("request to get all logical sources");
    auto event = RequestProcessor::GetAllLogicalSourcesEvent::create();
    auto request = RequestProcessor::GetSourceCatalogRequest::create(event, RequestProcessor::DEFAULT_RETRIES);
    asyncRequestExecutor->runAsync(request);
    auto future = request->getFuture();
    auto response = future.get();
    return std::static_pointer_cast<RequestProcessor::BaseGetSourceJsonResponse>(response)->getJson();
}

nlohmann::json RequestHandlerService::queueGetPhysicalSourcesRequest(std::string logicelSourceName) const {
    NES_DEBUG("request to get physical source");
    auto event = RequestProcessor::GetPhysicalSourcesEvent::create(logicelSourceName);
    auto request = RequestProcessor::GetSourceCatalogRequest::create(event, RequestProcessor::DEFAULT_RETRIES);
    asyncRequestExecutor->runAsync(request);
    auto future = request->getFuture();
    auto response = future.get();
    return std::static_pointer_cast<RequestProcessor::BaseGetSourceJsonResponse>(response)->getJson();
}

SchemaPtr RequestHandlerService::queueGetLogicalSourceSchemaRequest(std::string logicelSourceName) const {
    NES_DEBUG("request to get logical source schema");
    auto event = RequestProcessor::GetSchemaEvent::create(logicelSourceName);
    auto request = RequestProcessor::GetSourceCatalogRequest::create(event, RequestProcessor::DEFAULT_RETRIES);
    asyncRequestExecutor->runAsync(request);
    auto future = request->getFuture();
    auto response = future.get();
    return std::static_pointer_cast<RequestProcessor::GetSchemaResponse>(response)->getSchema();
}

bool RequestHandlerService::handleCatalogUpdateRequest(RequestProcessor::UpdateSourceCatalogEventPtr event) const {
    auto updateRequest = RequestProcessor::UpdateSourceCatalogRequest::create(event, RequestProcessor::DEFAULT_RETRIES);
    asyncRequestExecutor->runAsync(updateRequest);
    auto future = updateRequest->getFuture();
    auto response = future.get();
    return std::static_pointer_cast<RequestProcessor::BaseUpdateSourceCatalogResponse>(response)->success;
}
}// namespace NES

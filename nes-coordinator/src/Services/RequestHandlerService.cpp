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

#include <Catalogs/Exceptions/InvalidQueryException.hpp>
#include <Catalogs/Exceptions/InvalidQueryStateException.hpp>
#include <Catalogs/Query/QueryCatalog.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/UDF/UDFCatalog.hpp>
#include <Configurations/Coordinator/OptimizerConfiguration.hpp>
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
#include <RequestProcessor/RequestTypes/StopQueryRequest.hpp>
#include <RequestProcessor/RequestTypes/TopologyNodeRelocationRequest.hpp>
#include <Services/RequestHandlerService.hpp>
#include <Util/Core.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Placement/PlacementStrategy.hpp>
#include <nlohmann/json.hpp>

namespace NES {

RequestHandlerService::RequestHandlerService(Configurations::OptimizerConfiguration optimizerConfiguration,
                                             const QueryParsingServicePtr& queryParsingService,
                                             const Catalogs::Query::QueryCatalogPtr& queryCatalog,
                                             const Catalogs::Source::SourceCatalogPtr& sourceCatalog,
                                             const Catalogs::UDF::UDFCatalogPtr& udfCatalog,
                                             const NES::RequestProcessor::AsyncRequestProcessorPtr& asyncRequestExecutor,
                                             const z3::ContextPtr& z3Context)
    : optimizerConfiguration(optimizerConfiguration), queryParsingService(queryParsingService), queryCatalog(queryCatalog),
      asyncRequestExecutor(asyncRequestExecutor), z3Context(z3Context) {
    NES_DEBUG("RequestHandlerService()");
    syntacticQueryValidation = Optimizer::SyntacticQueryValidation::create(this->queryParsingService);
    semanticQueryValidation = Optimizer::SemanticQueryValidation::create(sourceCatalog,
                                                                         udfCatalog,
                                                                         optimizerConfiguration.performAdvanceSemanticValidation);
}

QueryId RequestHandlerService::validateAndQueueAddQueryRequest(const std::string& queryString,
                                                               const Optimizer::PlacementStrategy placementStrategy) {

    auto addRequest = RequestProcessor::AddQueryRequest::create(queryString,
                                                                placementStrategy,
                                                                RequestProcessor::DEFAULT_RETRIES,
                                                                z3Context,
                                                                queryParsingService);
    asyncRequestExecutor->runAsync(addRequest);
    auto future = addRequest->getFuture();
    return std::static_pointer_cast<RequestProcessor::AddQueryResponse>(future.get())->queryId;
}

QueryId RequestHandlerService::validateAndQueueAddQueryRequest(const QueryPlanPtr& queryPlan,
                                                               const Optimizer::PlacementStrategy placementStrategy) {

    auto addRequest =
        RequestProcessor::AddQueryRequest::create(queryPlan, placementStrategy, RequestProcessor::DEFAULT_RETRIES, z3Context);
    asyncRequestExecutor->runAsync(addRequest);
    auto future = addRequest->getFuture();
    return std::static_pointer_cast<RequestProcessor::AddQueryResponse>(future.get())->queryId;
}

nlohmann::json RequestHandlerService::validateAndQueueExplainQueryRequest(const NES::QueryPlanPtr& queryPlan,
                                                                          const Optimizer::PlacementStrategy placementStrategy) {

    auto explainRequest = RequestProcessor::ExplainRequest::create(queryPlan, placementStrategy, 1, z3Context);
    asyncRequestExecutor->runAsync(explainRequest);
    auto future = explainRequest->getFuture();
    return std::static_pointer_cast<RequestProcessor::ExplainResponse>(future.get())->jsonResponse;
}

bool RequestHandlerService::validateAndQueueStopQueryRequest(QueryId queryId) {

    try {
        auto stopRequest = RequestProcessor::StopQueryRequest::create(queryId, RequestProcessor::DEFAULT_RETRIES);
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
                                                             DecomposedQueryPlanId querySubPlanId,
                                                             const std::string& failureReason) {

    auto failRequest = RequestProcessor::FailQueryRequest::create(sharedQueryId,
                                                                  querySubPlanId,
                                                                  failureReason,
                                                                  RequestProcessor::DEFAULT_RETRIES);
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

bool RequestHandlerService::queueNodeRelocationRequest(const std::vector<TopologyLinkInformation>& removedLinks,
                                                       const std::vector<TopologyLinkInformation>& addedLinks) {
    auto nodeRelocationRequest =
        RequestProcessor::Experimental::TopologyNodeRelocationRequest::create(removedLinks,
                                                                              addedLinks,
                                                                              RequestProcessor::DEFAULT_RETRIES);
    asyncRequestExecutor->runAsync(nodeRelocationRequest);
    auto future = nodeRelocationRequest->getFuture();
    auto changeResponse =
        std::static_pointer_cast<RequestProcessor::Experimental::TopologyNodeRelocationRequestResponse>(future.get());
    return changeResponse->success;
}

bool RequestHandlerService::queueISQPRequest(const std::vector<RequestProcessor::ISQPEventPtr>& isqpEvents) {

    auto isqpRequest = RequestProcessor::ISQPRequest::create(z3Context, isqpEvents, RequestProcessor::DEFAULT_RETRIES);
    asyncRequestExecutor->runAsync(isqpRequest);
    auto future = isqpRequest->getFuture();
    auto changeResponse = std::static_pointer_cast<RequestProcessor::ISQPRequestResponse>(future.get());
    return changeResponse->success;
}

}// namespace NES

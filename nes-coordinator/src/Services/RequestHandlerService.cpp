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

#include "Compiler/CPPCompiler/CPPCompiler.hpp"
#include <Catalogs/Exceptions/InvalidQueryException.hpp>
#include <Catalogs/Exceptions/InvalidQueryStateException.hpp>
#include <Catalogs/Query/QueryCatalog.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/UDF/UDFCatalog.hpp>
#include <Configurations/Coordinator/OptimizerConfiguration.hpp>
#include <Operators/LogicalOperators/Sinks/NullOutputSinkDescriptor.hpp>
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
#include <RequestProcessor/RequestTypes/ISQP/ISQPEvents/ISQPAddQueryEvent.hpp>
#include <RequestProcessor/RequestTypes/ISQP/ISQPRequest.hpp>
#include <RequestProcessor/RequestTypes/StopQueryRequest.hpp>
#include <RequestProcessor/RequestTypes/TopologyNodeRelocationRequest.hpp>
#include <Services/RequestHandlerService.hpp>
#include <Util/Core.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Placement/PlacementStrategy.hpp>
#include <nlohmann/json.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Services/QueryParsingService.hpp>

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

void compileQuery(const std::string& stringQuery,
                  uint64_t id,
                  const std::shared_ptr<QueryParsingService>& queryParsingService,
                  std::promise<QueryPlanPtr> promise) {
    auto queryplan = queryParsingService->createQueryFromCodeString(stringQuery);
    queryplan->setQueryId(id);
    promise.set_value(queryplan);
}

void RequestHandlerService::validateAndQueueMultiQueryRequestParallel(std::vector<std::string> queries) {
    //using thread pool to parallelize the compilation of string queries and string them in an array of query objects
    const uint32_t numOfQueries = queries.size();
    QueryPlanPtr queryObjects[numOfQueries];

    auto cppCompiler = Compiler::CPPCompiler::create();
    auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
//    auto queryParsingService = QueryParsingService::create(jitCompiler);

    //If no available thread then set number of threads to 1
    uint64_t numThreads = std::thread::hardware_concurrency();
    if (numThreads == 0) {
        NES_WARNING("No available threads. Going to use only 1 thread for parsing input queries.");
        numThreads = 1;
    }
    std::cout << "Using " << numThreads << " of threads for parallel parsing." << std::endl;

    uint64_t queryNum = 0;
    //Work till all queries are not parsed
    while (queryNum < numOfQueries) {
        NES_ERROR("Parsing queries from {} to {}", queryNum, queryNum + numThreads - 1);
        std::vector<std::future<QueryPlanPtr>> futures;
        std::vector<std::thread> threadPool(numThreads);
        uint64_t threadNum;
        //Schedule queries to be parsed with #numThreads parallelism
        for (threadNum = 0; threadNum < numThreads; threadNum++) {
            //If no more query to parse
            if (queryNum >= numOfQueries) {
                break;
            }
            //Schedule thread for execution and pass a promise
            std::promise<QueryPlanPtr> promise;
            //Store the future, schedule the thread, and increment the query count
            futures.emplace_back(promise.get_future());
            threadPool.emplace_back(
                std::thread(compileQuery, queries[queryNum], queryNum + 1, queryParsingService, std::move(promise)));
            queryNum++;
        }

        //Wait for all unfinished threads
        for (auto& item : threadPool) {
            if (item.joinable()) {// if thread is not finished yet
                item.join();
            }
        }

        //Fetch the parsed query from all threads
        for (uint64_t futureNum = 0; futureNum < threadNum; futureNum++) {
            auto query = futures[futureNum].get();
            auto queryID = query->getQueryId();
            queryObjects[queryID - 1] = query;//Add the parsed query to the (queryID - 1)th index
        }
    }
    NES_ERROR("adding {} queries using isqp request", queries.size());
    //vector of add query events
    std::vector<RequestProcessor::ISQPEventPtr> isqpEvents;
    for (const auto& query : queryObjects) {
        auto addQueryEvent = RequestProcessor::ISQPAddQueryEvent::create(query, Optimizer::PlacementStrategy::BottomUp);
        isqpEvents.push_back(addQueryEvent);
    }
    NES_ERROR("queue isqp request");
    auto isqpRequest = RequestProcessor::ISQPRequest::create(z3Context, isqpEvents, RequestProcessor::DEFAULT_RETRIES, true);
    auto future = isqpRequest->getFuture();
    asyncRequestExecutor->runAsync(isqpRequest);
    auto changeResponse = std::static_pointer_cast<RequestProcessor::ISQPRequestResponse>(future.get());
}

void RequestHandlerService::validateAndQueueMultiQueryRequest(std::vector<std::pair<std::string, Optimizer::PlacementStrategy>> queryStrings, uint64_t duplicationFactor) {

    NES_ERROR("adding {} queries using isqp request", queryStrings.size());
    //vector of add query events
    std::vector<RequestProcessor::ISQPEventPtr> isqpEvents;

    auto count = 0;
    for (const auto& query : queryStrings) {
        NES_ERROR("validating query {}", count);
        auto queryPlan = syntacticQueryValidation->validate(query.first);
        if (queryPlan == nullptr) {
            NES_THROW_RUNTIME_ERROR("Query validation failed.");
        }
        auto addQueryEvent = RequestProcessor::ISQPAddQueryEvent::create(queryPlan, query.second);
        isqpEvents.push_back(addQueryEvent);
        NES_ERROR("added query plan {}", queryPlan->toString());
        count++;
        auto duplicatedQueryPlan = queryPlan->copy();
        auto nullOutputSink = NullOutputSinkDescriptor::create();
        auto child = duplicatedQueryPlan->getRootOperators().front()->getChildren().front();
        auto nullQueryPlan = QueryPlan::create();
//        nullQueryPlan->addRootOperator(nullOutputSink);

        for (uint64_t i = 1; i < duplicationFactor; i++) {
            isqpEvents.push_back(addQueryEvent);
        }
    }
    NES_ERROR("queue isqp request");
    auto isqpRequest = RequestProcessor::ISQPRequest::create(z3Context, isqpEvents, RequestProcessor::DEFAULT_RETRIES, true);
    asyncRequestExecutor->runAsync(isqpRequest);
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

RequestProcessor::ISQPRequestResponsePtr
RequestHandlerService::queueISQPRequest(const std::vector<RequestProcessor::ISQPEventPtr>& isqpEvents, bool waitForResponse) {

    auto isqpRequest = RequestProcessor::ISQPRequest::create(z3Context, isqpEvents, RequestProcessor::DEFAULT_RETRIES, true);
    auto future = isqpRequest->getFuture();
    asyncRequestExecutor->runAsync(isqpRequest);
    if (waitForResponse) {
        auto changeResponse = std::static_pointer_cast<RequestProcessor::ISQPRequestResponse>(future.get());
        return changeResponse;
    }
    return {};
}

bool RequestHandlerService::isIncrementalPlacementEnabled() {
    return optimizerConfiguration.enableIncrementalPlacement;
}
}// namespace NES

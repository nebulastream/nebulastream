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
#include <Catalogs/Query/QueryCatalogService.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/UDF/UDFCatalog.hpp>
#include <Configurations/Coordinator/OptimizerConfiguration.hpp>
#include <Exceptions/InvalidArgumentException.hpp>
#include <Optimizer/QueryPlacement/PlacementStrategyFactory.hpp>
#include <Optimizer/RequestTypes/QueryRequests/AddQueryRequest.hpp>
#include <Optimizer/RequestTypes/QueryRequests/FailQueryRequest.hpp>
#include <Optimizer/RequestTypes/QueryRequests/StopQueryRequest.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/PlanIdGenerator.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <QueryValidation/SemanticQueryValidation.hpp>
#include <QueryValidation/SyntacticQueryValidation.hpp>
#include <RequestProcessor/AsyncRequestProcessor.hpp>
#include <RequestProcessor/RequestTypes/AddQueryRequest.hpp>
#include <RequestProcessor/RequestTypes/FailQueryRequest.hpp>
#include <RequestProcessor/RequestTypes/StopQueryRequest.hpp>
#include <Services/QueryService.hpp>
#include <Util/Core.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/PlacementStrategy.hpp>
#include <WorkQueues/RequestQueue.hpp>

namespace NES {

QueryService::QueryService(bool enableNewRequestExecutor,
                           Configurations::OptimizerConfiguration optimizerConfiguration,
                           const QueryCatalogServicePtr& queryCatalogService,
                           const RequestQueuePtr& queryRequestQueue,
                           const Catalogs::Source::SourceCatalogPtr& sourceCatalog,
                           const QueryParsingServicePtr& queryParsingService,
                           const Catalogs::UDF::UDFCatalogPtr& udfCatalog,
                           const RequestProcessor::Experimental::AsyncRequestProcessorPtr& asyncRequestExecutor,
                           const z3::ContextPtr& z3Context)
    : enableNewRequestExecutor(enableNewRequestExecutor), optimizerConfiguration(optimizerConfiguration),
      queryCatalogService(queryCatalogService), queryRequestQueue(queryRequestQueue), asyncRequestExecutor(asyncRequestExecutor),
      z3Context(z3Context), queryParsingService(queryParsingService) {
    NES_DEBUG("QueryService()");
    syntacticQueryValidation = Optimizer::SyntacticQueryValidation::create(this->queryParsingService);
    semanticQueryValidation = Optimizer::SemanticQueryValidation::create(sourceCatalog,
                                                                         udfCatalog,
                                                                         optimizerConfiguration.performAdvanceSemanticValidation);
}

QueryId QueryService::validateAndQueueAddQueryRequest(const std::string& queryString,
                                                      const Optimizer::PlacementStrategy placementStrategy) {

    if (!enableNewRequestExecutor) {
        NES_INFO("QueryService: Validating and registering the user query.");
        QueryId queryId = PlanIdGenerator::getNextQueryId();
        try {
            // Checking the syntactic validity and compiling the query string to an object
            NES_INFO("QueryService: check validation of a query.");
            QueryPlanPtr queryPlan = syntacticQueryValidation->validate(queryString);

            queryPlan->setQueryId(queryId);
            queryPlan->setPlacementStrategy(placementStrategy);

            // perform semantic validation
            semanticQueryValidation->validate(queryPlan);

            Catalogs::Query::QueryCatalogEntryPtr queryCatalogEntry =
                queryCatalogService->createNewEntry(queryString, queryPlan, placementStrategy);
            if (queryCatalogEntry) {
                auto request = AddQueryRequest::create(queryPlan, placementStrategy);
                queryRequestQueue->add(request);
                return queryId;
            }
        } catch (const InvalidQueryException& exc) {
            NES_ERROR("QueryService: {}", std::string(exc.what()));
            auto emptyQueryPlan = QueryPlan::create();
            emptyQueryPlan->setQueryId(queryId);
            queryCatalogService->createNewEntry(queryString, emptyQueryPlan, placementStrategy);
            queryCatalogService->updateQueryStatus(queryId, QueryState::FAILED, exc.what());
            throw exc;
        }
        throw Exceptions::RuntimeException("QueryService: unable to create query catalog entry");
    } else {

        auto addRequest = RequestProcessor::Experimental ::AddQueryRequest::create(queryString,
                                                                                   placementStrategy,
                                                                                   1,
                                                                                   z3Context,
                                                                                   queryParsingService);
        asyncRequestExecutor->runAsync(addRequest);
        auto future = addRequest->getFuture();
        return std::static_pointer_cast<RequestProcessor::Experimental::AddQueryResponse>(future.get())->queryId;
    }
}

QueryId QueryService::validateAndQueueAddQueryRequest(const std::string& queryString,
                                      const QueryPlanPtr& queryPlan,
                                      const Optimizer::PlacementStrategy placementStrategy) {

    if (!enableNewRequestExecutor) {
        QueryId queryId = PlanIdGenerator::getNextQueryId();
        auto promise = std::make_shared<std::promise<QueryId>>();
        try {

            //Assign additional configurations
            queryPlan->setQueryId(queryId);
            queryPlan->setPlacementStrategy(placementStrategy);

            // assign the id for the query and individual operators
            assignOperatorIds(queryPlan);

            // perform semantic validation
            semanticQueryValidation->validate(queryPlan);

            Catalogs::Query::QueryCatalogEntryPtr queryCatalogEntry =
                queryCatalogService->createNewEntry(queryString, queryPlan, placementStrategy);
            if (queryCatalogEntry) {
                auto request = AddQueryRequest::create(queryPlan, placementStrategy);
                queryRequestQueue->add(request);
                return queryId;
            }
        } catch (const InvalidQueryException& exc) {
            NES_ERROR("QueryService: {}", std::string(exc.what()));
            auto emptyQueryPlan = QueryPlan::create();
            emptyQueryPlan->setQueryId(queryId);
            queryCatalogService->createNewEntry(queryString, emptyQueryPlan, placementStrategy);
            queryCatalogService->updateQueryStatus(queryId, QueryState::FAILED, exc.what());
            throw exc;
        }
        throw Exceptions::RuntimeException("QueryService: unable to create query catalog entry");
    } else {
        auto addRequest = RequestProcessor::Experimental::AddQueryRequest::create(queryPlan, placementStrategy, 1, z3Context);
        asyncRequestExecutor->runAsync(addRequest);
        auto future = addRequest->getFuture();
        return std::static_pointer_cast<RequestProcessor::Experimental::AddQueryResponse>(future.get())->queryId;
    }
}

QueryId QueryService::validateAndQueueExplainQueryRequest(const NES::QueryPlanPtr& queryPlan,
                                                          const Optimizer::PlacementStrategy placementStrategy) {

    if (enableNewRequestExecutor) {
        auto addRequest = RequestProcessor::Experimental::AddQueryRequest::create(queryPlan,
                                                                                  placementStrategy,
                                                                                  1,
                                                                                  z3Context);
        asyncRequestExecutor->runAsync(addRequest);
        auto future = addRequest->getFuture();
        return std::static_pointer_cast<RequestProcessor::Experimental::AddQueryResponse>(future.get())->queryId;
    } else {
        NES_NOT_IMPLEMENTED();
    }
}

bool QueryService::validateAndQueueStopQueryRequest(QueryId queryId) {

    if (!enableNewRequestExecutor) {
        //Check and mark query for hard stop
        bool success = queryCatalogService->checkAndMarkForHardStop(queryId);

        //If success then queue the hard stop request
        if (success) {
            auto request = StopQueryRequest::create(queryId);
            return queryRequestQueue->add(request);
        }
        return false;
    } else {
        auto stopRequest = RequestProcessor::Experimental::StopQueryRequest::create(queryId, 1);
        auto future = stopRequest->getFuture();
        asyncRequestExecutor->runAsync(stopRequest);
        try {
            auto response = future.get();
            auto success = std::static_pointer_cast<RequestProcessor::Experimental::StopQueryResponse>(response)->success;
            return success;
        } catch (Exceptions::InvalidQueryStateException& e) {
            //return true if the query was already stopped
            return e.getActualState() == QueryState::STOPPED;
        }
    }
}

bool QueryService::validateAndQueueFailQueryRequest(SharedQueryId sharedQueryId,
                                                    QuerySubPlanId querySubPlanId,
                                                    const std::string& failureReason) {

    if (!enableNewRequestExecutor) {
        auto request = FailQueryRequest::create(sharedQueryId, failureReason);
        return queryRequestQueue->add(request);
    } else {
        auto failRequest = RequestProcessor::Experimental::FailQueryRequest::create(sharedQueryId, querySubPlanId, 1);
        auto future = failRequest->getFuture();
        asyncRequestExecutor->runAsync(failRequest);
        auto returnedSharedQueryId =
            std::static_pointer_cast<RequestProcessor::Experimental::FailQueryResponse>(future.get())->sharedQueryId;
        return returnedSharedQueryId != INVALID_SHARED_QUERY_ID;
    }
}

void QueryService::assignOperatorIds(QueryPlanPtr queryPlan) {
    // Iterate over all operators in the query and replace the client-provided ID
    auto queryPlanIterator = QueryPlanIterator(queryPlan);
    for (auto itr = queryPlanIterator.begin(); itr != QueryPlanIterator::end(); ++itr) {
        auto visitingOp = (*itr)->as<OperatorNode>();
        visitingOp->setId(getNextOperatorId());
    }
}

}// namespace NES

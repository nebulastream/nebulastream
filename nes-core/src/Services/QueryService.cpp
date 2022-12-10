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

#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/UDF/UdfCatalog.hpp>
#include <Exceptions/InvalidArgumentException.hpp>
#include <Exceptions/InvalidQueryException.hpp>
#include <Optimizer/QueryPlacement/PlacementStrategyFactory.hpp>
#include <Optimizer/QueryValidation/SemanticQueryValidation.hpp>
#include <Optimizer/QueryValidation/SyntacticQueryValidation.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/PlanIdGenerator.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <Services/QueryCatalogService.hpp>
#include <Services/QueryService.hpp>
#include <Util/PlacementStrategy.hpp>
#include <Util/UtilityFunctions.hpp>
#include <WorkQueues/RequestQueue.hpp>
#include <WorkQueues/RequestTypes/FailQueryRequest.hpp>
#include <WorkQueues/RequestTypes/RunQueryRequest.hpp>
#include <WorkQueues/RequestTypes/StopQueryRequest.hpp>

#include <utility>

namespace NES {

QueryService::QueryService(QueryCatalogServicePtr queryCatalogService,
                           RequestQueuePtr queryRequestQueue,
                           Catalogs::Source::SourceCatalogPtr sourceCatalog,
                           QueryParsingServicePtr queryParsingService,
                           Configurations::OptimizerConfiguration optimizerConfiguration,
                           Catalogs::UDF::UdfCatalogPtr udfCatalog)
    : queryCatalogService(std::move(queryCatalogService)), queryRequestQueue(std::move(queryRequestQueue)),
      optimizerConfiguration(optimizerConfiguration) {
    NES_DEBUG("QueryService()");
    syntacticQueryValidation = Optimizer::SyntacticQueryValidation::create(std::move(queryParsingService));
    semanticQueryValidation = Optimizer::SemanticQueryValidation::create(std::move(sourceCatalog),
                                                                         optimizerConfiguration.performAdvanceSemanticValidation,
                                                                         udfCatalog);
}

QueryId QueryService::validateAndQueueAddQueryRequest(const std::string& queryString,
                                                      const std::string& placementStrategyName,
                                                      const FaultToleranceType::Value faultTolerance,
                                                      const LineageType::Value lineage) {

    NES_INFO("QueryService: Validating and registering the user query.");
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    try {
        // Checking the syntactic validity and compiling the query string to an object
        NES_INFO("QueryService: check validation of a query.");
        QueryPlanPtr queryPlan = syntacticQueryValidation->validate(queryString);

        queryPlan->setQueryId(queryId);
        queryPlan->setFaultToleranceType(faultTolerance);
        queryPlan->setLineageType(lineage);

        // perform semantic validation
        semanticQueryValidation->validate(queryPlan);

        PlacementStrategy::Value placementStrategy;
        try {
            placementStrategy = PlacementStrategy::getFromString(placementStrategyName);
        } catch (...) {
            NES_ERROR("QueryService: Unknown placement strategy name: " + placementStrategyName);
            throw InvalidArgumentException("placementStrategyName", placementStrategyName);
        }

        Catalogs::Query::QueryCatalogEntryPtr queryCatalogEntry =
            queryCatalogService->createNewEntry(queryString, queryPlan, placementStrategyName);
        if (queryCatalogEntry) {
            auto request = RunQueryRequest::create(queryPlan, placementStrategy);
            queryRequestQueue->add(request);
            return queryId;
        }
    } catch (const InvalidQueryException& exc) {
        NES_ERROR("QueryService: " + std::string(exc.what()));
        auto emptyQueryPlan = QueryPlan::create();
        emptyQueryPlan->setQueryId(queryId);
        queryCatalogService->createNewEntry(queryString, emptyQueryPlan, placementStrategyName);
        queryCatalogService->updateQueryStatus(queryId, QueryStatus::Failed, exc.what());
        throw exc;
    }
    throw Exceptions::RuntimeException("QueryService: unable to create query catalog entry");
}

QueryId QueryService::addQueryRequest(const std::string& queryString,
                                      const QueryPlanPtr& queryPlan,
                                      const std::string& placementStrategyName,
                                      const FaultToleranceType::Value faultTolerance,
                                      const LineageType::Value lineage) {

    QueryId queryId = PlanIdGenerator::getNextQueryId();
    auto promise = std::make_shared<std::promise<QueryId>>();
    try {

        //Assign additional configurations
        queryPlan->setQueryId(queryId);
        queryPlan->setFaultToleranceType(faultTolerance);
        queryPlan->setLineageType(lineage);

        // assign the id for the query and individual operators
        assignOperatorIds(queryPlan);

        // perform semantic validation
        semanticQueryValidation->validate(queryPlan);

        PlacementStrategy::Value placementStrategy;
        try {
            placementStrategy = PlacementStrategy::getFromString(placementStrategyName);
        } catch (...) {
            NES_ERROR("QueryService: Unknown placement strategy name: " + placementStrategyName);
            throw InvalidArgumentException("placementStrategyName", placementStrategyName);
        }

        Catalogs::Query::QueryCatalogEntryPtr queryCatalogEntry =
            queryCatalogService->createNewEntry(queryString, queryPlan, placementStrategyName);
        if (queryCatalogEntry) {
            auto request = RunQueryRequest::create(queryPlan, placementStrategy);
            queryRequestQueue->add(request);
            return queryId;
        }
    } catch (const InvalidQueryException& exc) {
        NES_ERROR("QueryService: " + std::string(exc.what()));
        auto emptyQueryPlan = QueryPlan::create();
        emptyQueryPlan->setQueryId(queryId);
        queryCatalogService->createNewEntry(queryString, emptyQueryPlan, placementStrategyName);
        queryCatalogService->updateQueryStatus(queryId, QueryStatus::Failed, exc.what());
        throw exc;
    }
    throw Exceptions::RuntimeException("QueryService: unable to create query catalog entry");
}

bool QueryService::validateAndQueueStopQueryRequest(QueryId queryId) {

    //Check and mark query for hard stop
    bool success = queryCatalogService->checkAndMarkForHardStop(queryId);

    //If success then queue the hard stop request
    if (success) {
        auto request = StopQueryRequest::create(queryId);
        return queryRequestQueue->add(request);
    }
    return false;
}

bool QueryService::validateAndQueueFailQueryRequest(SharedQueryId sharedQueryId, const std::string& failureReason) {
    auto request = FailQueryRequest::create(sharedQueryId, failureReason);
    return queryRequestQueue->add(request);
}

void QueryService::assignOperatorIds(QueryPlanPtr queryPlan) {
    // Iterate over all operators in the query and replace the client-provided ID
    auto queryPlanIterator = QueryPlanIterator(queryPlan);
    for (auto itr = queryPlanIterator.begin(); itr != QueryPlanIterator::end(); ++itr) {
        auto visitingOp = (*itr)->as<OperatorNode>();
        visitingOp->setId(Util::getNextOperatorId());
    }
}

}// namespace NES

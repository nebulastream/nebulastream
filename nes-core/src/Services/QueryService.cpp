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
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Exceptions/InvalidArgumentException.hpp>
#include <Exceptions/InvalidQueryException.hpp>
#include <Exceptions/QueryNotFoundException.hpp>
#include <Optimizer/QueryPlacement/PlacementStrategyFactory.hpp>
#include <Optimizer/QueryValidation/SemanticQueryValidation.hpp>
#include <Optimizer/QueryValidation/SyntacticQueryValidation.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/PlanIdGenerator.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <Services/QueryService.hpp>
#include <Util/PlacementStrategy.hpp>
#include <Util/UtilityFunctions.hpp>
#include <WorkQueues/RequestQueue.hpp>
#include <WorkQueues/RequestTypes/RunQueryRequest.hpp>
#include <WorkQueues/RequestTypes/StopQueryRequest.hpp>
#include <utility>

namespace NES {

QueryService::QueryService(QueryCatalogPtr queryCatalog,
                           RequestQueuePtr queryRequestQueue,
                           SourceCatalogPtr streamCatalog,
                           std::shared_ptr<QueryParsingService> queryParsingService,
                           bool enableSemanticQueryValidation)
    : queryCatalog(std::move(queryCatalog)), queryRequestQueue(std::move(queryRequestQueue)),
      enableSemanticQueryValidation(enableSemanticQueryValidation) {
    NES_DEBUG("QueryService()");
    syntacticQueryValidation = Optimizer::SyntacticQueryValidation::create(std::move(queryParsingService));
    semanticQueryValidation = Optimizer::SemanticQueryValidation::create(streamCatalog);
}

uint64_t QueryService::validateAndQueueAddRequest(const std::string& queryString,
                                                  const std::string& placementStrategyName,
                                                  const FaultToleranceType faultTolerance,
                                                  const LineageType lineage) {

    NES_INFO("QueryService: Validating and registering the user query.");
    QueryId queryId = PlanIdGenerator::getNextQueryId();

    NES_INFO("QueryService: Executing Syntactic validation");
    QueryPtr query;
    try {
        // Checking the syntactic validity and compiling the query string to an object
        NES_INFO("QueryService: check validation of a query");
        query = syntacticQueryValidation->checkValidityAndGetQuery(queryString);
    } catch (const std::exception& exc) {
        NES_ERROR("QueryService: Syntactic Query Validation: " + std::string(exc.what()));
        // On compilation error we record the query to the catalog as failed
        queryCatalog->recordInvalidQuery(queryString, queryId, QueryPlan::create(), placementStrategyName);
        queryCatalog->setQueryFailureReason(queryId, exc.what());
        throw InvalidQueryException(exc.what());
    }

    PlacementStrategy::Value placementStrategy;
    NES_INFO("QueryService: Validating placement strategy name.");
    try {
        placementStrategy = PlacementStrategy::getFromString(placementStrategyName);
    } catch (...) {
        NES_ERROR("QueryService: Unknown placement strategy name: " + placementStrategyName);
        throw InvalidArgumentException("placementStrategyName", placementStrategyName);
    }

    QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(queryId);
    queryPlan->setFaultToleranceType(faultTolerance);
    queryPlan->setLineageType(lineage);

    // Execute only if the semantic validation flag is enabled
    if (enableSemanticQueryValidation) {
        NES_INFO("QueryService: Executing Semantic validation");
        try {
            // Checking semantic validity
            semanticQueryValidation->checkSatisfiability(query);
        } catch (const std::exception& exc) {
            // If semantically invalid, we record the query to the catalog as failed
            NES_ERROR("QueryService: Semantic Query Validation: " + std::string(exc.what()));
            queryCatalog->recordInvalidQuery(queryString, queryId, queryPlan, placementStrategyName);
            queryCatalog->setQueryFailureReason(queryId, exc.what());
            throw InvalidQueryException(exc.what());
        }
    }

    NES_INFO("QueryService: Queuing the query for the execution");
    QueryCatalogEntryPtr entry = queryCatalog->addNewQuery(queryString, queryPlan, placementStrategyName);
    if (entry) {
        auto request = RunQueryRequest::create(queryPlan, placementStrategy);
        queryRequestQueue->add(request);
        return queryId;
    }
    throw Exception("QueryService: unable to create query catalog entry");
}

bool QueryService::validateAndQueueStopRequest(QueryId queryId) {
    if (!queryCatalog->queryExists(queryId)) {
        throw QueryNotFoundException("QueryService: Unable to find query with id " + std::to_string(queryId)
                                     + " in query catalog.");
    }
    QueryCatalogEntryPtr entry = queryCatalog->stopQuery(queryId);
    if (entry) {
        auto request = StopQueryRequest::create(queryId);
        return queryRequestQueue->add(request);
    }
    return false;
}

uint64_t QueryService::addQueryRequest(const std::string& queryString,
                                       Query query,
                                       const std::string& placementStrategyName,
                                       const FaultToleranceType faultTolerance,
                                       const LineageType lineage) {
    NES_INFO("QueryService: Queuing the query for the execution");
    auto queryPlan = query.getQueryPlan();
    queryPlan->setFaultToleranceType(faultTolerance);
    queryPlan->setLineageType(lineage);
    QueryCatalogEntryPtr entry = queryCatalog->addNewQuery(queryString, queryPlan, placementStrategyName);
    if (entry) {
        PlacementStrategy::Value placementStrategy = PlacementStrategy::getFromString(placementStrategyName);
        auto request = RunQueryRequest::create(queryPlan, placementStrategy);
        queryRequestQueue->add(request);
        return queryPlan->getQueryId();
    }
    throw Exception("QueryService: unable to create query catalog entry");
}

uint64_t QueryService::addQueryRequest(const QueryPlanPtr& queryPlan,
                                       const std::string& placementStrategyName,
                                       const FaultToleranceType faultTolerance,
                                       const LineageType lineage) {
    try {
        QueryCatalogEntryPtr entry = queryCatalog->addNewQuery("", queryPlan, placementStrategyName);
        queryPlan->setFaultToleranceType(faultTolerance);
        queryPlan->setLineageType(lineage);
        if (entry) {
            PlacementStrategy::Value placementStrategy = PlacementStrategy::getFromString(placementStrategyName);
            auto request = RunQueryRequest::create(queryPlan, placementStrategy);
            queryRequestQueue->add(request);
            return queryPlan->getQueryId();
        }
    } catch (...) {
        throw Exception("QueryService: unable to create query catalog entry");
    }
    throw Exception("QueryService: unable to create query catalog entry");
}

uint64_t QueryService::addQueryRequest(const std::string& queryString,
                                       const QueryPlanPtr& queryPlan,
                                       const std::string& placementStrategyName,
                                       const FaultToleranceType faultTolerance,
                                       const LineageType lineage) {
    try {
        // assign the id for the query and individual operators
        assignQueryAndOperatorIds(queryPlan);
        QueryCatalogEntryPtr entry = queryCatalog->addNewQuery(queryString, queryPlan, placementStrategyName);
        queryPlan->setFaultToleranceType(faultTolerance);
        queryPlan->setLineageType(lineage);
        if (entry) {
            PlacementStrategy::Value placementStrategy = PlacementStrategy::getFromString(placementStrategyName);
            auto request = RunQueryRequest::create(queryPlan, placementStrategy);
            queryRequestQueue->add(request);
            return queryPlan->getQueryId();
        }
    } catch (...) {
        throw Exception("QueryService: unable to create query catalog entry");
    }
    throw Exception("QueryService: unable to create query catalog entry");
}

void QueryService::assignQueryAndOperatorIds(QueryPlanPtr queryPlan) {
    // Set the queryId
    queryPlan->setQueryId(PlanIdGenerator::getNextQueryId());

    // Iterate over all operators in the query and replace the client-provided ID
    auto queryPlanIterator = QueryPlanIterator(queryPlan);
    for (auto itr = queryPlanIterator.begin(); itr != QueryPlanIterator::end(); ++itr) {
        auto visitingOp = (*itr)->as<OperatorNode>();
        visitingOp->setId(Util::getNextOperatorId());
    }
}

}// namespace NES

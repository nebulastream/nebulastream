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
#include <log4cxx/helpers/exception.h>

namespace NES {

QueryService::QueryService(QueryCatalogPtr queryCatalog,
                           RequestQueuePtr queryRequestQueue,
                           SourceCatalogPtr sourceCatalog,
                           std::shared_ptr<QueryParsingService> queryParsingService,
                           Configurations::OptimizerConfiguration optimizerConfiguration)
    : queryCatalog(std::move(queryCatalog)), queryRequestQueue(std::move(queryRequestQueue)) {
    NES_DEBUG("QueryService()");
    syntacticQueryValidation = Optimizer::SyntacticQueryValidation::create(std::move(queryParsingService));
    semanticQueryValidation =
        Optimizer::SemanticQueryValidation::create(sourceCatalog, optimizerConfiguration.performAdvanceSemanticValidation);
}

uint64_t QueryService::validateAndQueueAddRequest(const std::string& queryString,
                                                  const std::string& placementStrategyName,
                                                  const FaultToleranceType faultTolerance,
                                                  const LineageType lineage) {

    NES_INFO("QueryService: Validating and registering the user query.");
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    try {
        // Checking the syntactic validity and compiling the query string to an object
        NES_INFO("QueryService: check validation of a query.");
        QueryPtr query = syntacticQueryValidation->validate(queryString);

        //Assign additional configurations
        QueryPlanPtr queryPlan = query->getQueryPlan();
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

        QueryCatalogEntryPtr queryCatalogEntry = queryCatalog->addNewQuery(queryString, queryPlan, placementStrategyName);
        if (queryCatalogEntry) {
            auto request = RunQueryRequest::create(queryPlan, placementStrategy);
            queryRequestQueue->add(request);
            return queryId;
        }
    } catch (const InvalidQueryException& exc) {
        NES_ERROR("QueryService: Syntactic Query Validation: " + std::string(exc.what()));
        queryCatalog->recordInvalidQuery(queryString, queryId, QueryPlan::create(), placementStrategyName);
        queryCatalog->setQueryFailureReason(queryId, exc.what());
        throw exc;
    }
    throw log4cxx::helpers::Exception("QueryService: unable to create query catalog entry");
}

uint64_t QueryService::addQueryRequest(const std::string& queryString,
                                       const QueryPlanPtr& queryPlan,
                                       const std::string& placementStrategyName,
                                       const FaultToleranceType faultTolerance,
                                       const LineageType lineage) {

    QueryId queryId = PlanIdGenerator::getNextQueryId();
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

        QueryCatalogEntryPtr queryCatalogEntry = queryCatalog->addNewQuery(queryString, queryPlan, placementStrategyName);
        if (queryCatalogEntry) {
            auto request = RunQueryRequest::create(queryPlan, placementStrategy);
            queryRequestQueue->add(request);
            return queryPlan->getQueryId();
        }
    } catch (const InvalidQueryException& exc) {
        NES_ERROR("QueryService: Syntactic Query Validation: " + std::string(exc.what()));
        queryCatalog->recordInvalidQuery(queryString, queryId, queryPlan, placementStrategyName);
        queryCatalog->setQueryFailureReason(queryId, exc.what());
        throw exc;
    }
    throw log4cxx::helpers::Exception("QueryService: unable to create query catalog entry");
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
    QueryCatalogEntryPtr queryCatalogEntry = queryCatalog->addNewQuery(queryString, queryPlan, placementStrategyName);
    if (queryCatalogEntry) {
        PlacementStrategy::Value placementStrategy = PlacementStrategy::getFromString(placementStrategyName);
        auto request = RunQueryRequest::create(queryPlan, placementStrategy);
        queryRequestQueue->add(request);
        return queryPlan->getQueryId();
    }
    throw log4cxx::helpers::Exception("QueryService: unable to create query catalog queryCatalogEntry");
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

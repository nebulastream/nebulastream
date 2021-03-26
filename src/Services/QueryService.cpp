/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <Catalogs/QueryCatalog.hpp>
#include <Exceptions/InvalidArgumentException.hpp>
#include <Exceptions/QueryNotFoundException.hpp>
#include <Exceptions/InvalidQueryException.hpp>
#include <Optimizer/QueryPlacement/PlacementStrategyFactory.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/PlanIdGenerator.hpp>
#include <Services/QueryService.hpp>
#include <Util/UtilityFunctions.hpp>
#include <WorkQueues/QueryRequestQueue.hpp>
#include <Optimizer/QueryValidation/SyntacticQueryValidation.hpp>
#include <Optimizer/QueryValidation/SemanticQueryValidation.hpp>
#include <Catalogs/StreamCatalog.hpp>


namespace NES {

QueryService::QueryService(QueryCatalogPtr queryCatalog, QueryRequestQueuePtr queryRequestQueue, StreamCatalogPtr streamCatalog, bool enableSemanticQueryValidation)
    : queryCatalog(queryCatalog), queryRequestQueue(queryRequestQueue), streamCatalog(streamCatalog), enableSemanticQueryValidation(enableSemanticQueryValidation) {
    NES_DEBUG("QueryService()");
}

QueryService::~QueryService() { NES_DEBUG("~QueryService()"); }

uint64_t QueryService::validateAndQueueAddRequest(std::string queryString, std::string placementStrategyName) {

    NES_INFO("QueryService: Validating and registering the user query.");
    QueryId queryId = PlanIdGenerator::getNextQueryId();

    NES_INFO("QueryService: Executing Syntactic validation");
    QueryPtr query;
    try{
        // Checking the syntactic validity and compiling the query string to an object 
        SyntacticQueryValidation syntacticQueryValidation;
        query = syntacticQueryValidation.checkValidityAndGetQuery(queryString);
    } catch (const std::exception& exc) {
        NES_ERROR("QueryService: Syntactic Query Validation: " + std::string(exc.what()));
        // On compilation error we record the query to the catalog as failed
        queryCatalog->recordInvalidQuery(queryString, queryId, QueryPlan::create(), placementStrategyName);
        queryCatalog->setQueryFailureReason(queryId, exc.what());
        throw InvalidQueryException(exc.what());
    }

    NES_INFO("QueryService: Validating placement strategy");
    if (stringToPlacementStrategyType.find(placementStrategyName) == stringToPlacementStrategyType.end()) {
        NES_ERROR("QueryService: Unknown placement strategy name: " + placementStrategyName);
        throw InvalidArgumentException("placementStrategyName", placementStrategyName);
    }
    
    const QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(queryId);

    // Execute only if the semantic validation flag is enabled
    if (enableSemanticQueryValidation){
        NES_INFO("QueryService: Executing Semantic validation");
        try{
            // Checking semantic validity
            SemanticQueryValidationPtr semanticQueryValidation = SemanticQueryValidation::create(streamCatalog);
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
    QueryCatalogEntryPtr entry = queryCatalog->addNewQueryRequest(queryString, queryPlan, placementStrategyName);
    if (entry) {
        queryRequestQueue->add(entry);
        return queryId;
    } else {
        throw Exception("QueryService: unable to create query catalog entry");
    }
}

bool QueryService::validateAndQueueStopRequest(QueryId queryId) {
    if (!queryCatalog->queryExists(queryId)) {
        throw QueryNotFoundException("QueryService: Unable to find query with id " + std::to_string(queryId)
                                     + " in query catalog.");
    }
    QueryCatalogEntryPtr entry = queryCatalog->addQueryStopRequest(queryId);
    if (entry) {
        return queryRequestQueue->add(entry);
    }
    return false;
}

uint64_t QueryService::addQueryRequest(std::string queryString, QueryPtr query, std::string placementStrategyName) {
    NES_INFO("QueryService: Queuing the query for the execution");
    auto queryPlan = query->getQueryPlan();
    QueryCatalogEntryPtr entry = queryCatalog->addNewQueryRequest(queryString, queryPlan, placementStrategyName);
    if (entry) {
        queryRequestQueue->add(entry);
        return queryPlan->getQueryId();
    } else {
        throw Exception("QueryService: unable to create query catalog entry");
    }
}

}// namespace NES

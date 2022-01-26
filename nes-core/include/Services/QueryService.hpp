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

#ifndef NES_INCLUDE_SERVICES_QUERYSERVICE_HPP_
#define NES_INCLUDE_SERVICES_QUERYSERVICE_HPP_

#include <API/Query.hpp>
#include <Plans/Query/QueryId.hpp>

namespace NES::Optimizer {
class SyntacticQueryValidation;
using SyntacticQueryValidationPtr = std::shared_ptr<SyntacticQueryValidation>;

class SemanticQueryValidation;
using SemanticQueryValidationPtr = std::shared_ptr<SemanticQueryValidation>;
}// namespace NES::Optimizer

namespace NES {

class QueryService;
using QueryServicePtr = std::shared_ptr<QueryService>;

class QueryCatalog;
using QueryCatalogPtr = std::shared_ptr<QueryCatalog>;

class RequestQueue;
using RequestQueuePtr = std::shared_ptr<RequestQueue>;

class QueryParsingService;
using QueryParsingServicePtr = std::shared_ptr<QueryParsingService>;

/**
 * @brief: This class is responsible for handling requests related to submitting, fetching information, and deleting different queries.
 */
class QueryService {

  public:
    explicit QueryService(QueryCatalogPtr queryCatalog,
                          RequestQueuePtr queryRequestQueue,
                          SourceCatalogPtr streamCatalog,
                          QueryParsingServicePtr queryParsingService,
                          bool enableSemanticQueryValidation);

    /**
     * Register the incoming query in the system by add it to the scheduling queue for further processing, and return the query Id assigned.
     * @param queryString : query in string form.
     * @param placementStrategyName : name of the placement strategy to be used.
     * @param faultTolerance : fault-tolerance guarantee for the given query.
     * @param lineage : lineage type for the given query.
     * @return queryId : query id of the valid input query.
     * @throws InvalidQueryException : when query string is not valid.
     * @throws InvalidArgumentException : when the placement strategy is not valid.
     */
    uint64_t validateAndQueueAddRequest(const std::string& queryString,
                                        const std::string& placementStrategyName,
                                        const FaultToleranceType faultTolerance = FaultToleranceType::NONE,
                                        const LineageType lineage = LineageType::NONE);

    /**
     * @deprecated NOT TO BE USED
     * @brief This method is used for submitting the queries directly to the system.
     * @param queryString : Query string
     * @param queryPtr : Query Object
     * @param placementStrategyName : Name of the placement strategy
     * @param faultTolerance : fault-tolerance guarantee for the given query.
     * @param lineage : lineage type for the given query.
     * @return query id
     */
    uint64_t addQueryRequest(const std::string& queryString,
                             Query query,
                             const std::string& placementStrategyName,
                             const FaultToleranceType faultTolerance = FaultToleranceType::NONE,
                             const LineageType lineage = LineageType::NONE);

    /**
     * @brief This method is used for submitting the queries directly to the system.
     * @param queryPlan : Query Plan Pointer Object
     * @param placementStrategyName : Name of the placement strategy
     * @param faultTolerance : fault-tolerance guarantee for the given query.
     * @param lineage : lineage type for the given query.
     * @return query id
     */
    uint64_t addQueryRequest(const QueryPlanPtr& queryPlan,
                             const std::string& placementStrategyName,
                             const FaultToleranceType faultTolerance = FaultToleranceType::NONE,
                             const LineageType lineage = LineageType::NONE);

    /**
     * @brief
     * @param queryString
     * @param queryPlan : Query Plan Pointer Object
     * @param placementStrategyName : Name of the placement strategy
     * @param faultTolerance : fault-tolerance guarantee for the given query.
     * @param lineage : lineage type for the given query.
     * @return query id
     */
    uint64_t addQueryRequest(const std::string& queryString,
                             const QueryPlanPtr& queryPlan,
                             const std::string& placementStrategyName,
                             const FaultToleranceType faultTolerance = FaultToleranceType::NONE,
                             const LineageType lineage = LineageType::NONE);

    /**
     * Register the incoming query in the system by add it to the scheduling queue for further processing, and return the query Id assigned.
     * @param queryId : query id of the query to be stopped.
     * @returns: true if successful
     * @throws QueryNotFoundException : when query id is not found in the query catalog.
     * @throws InvalidQueryStatusException : when the query is found to be in an invalid state.
     */
    bool validateAndQueueStopRequest(QueryId queryId);

  private:
    QueryCatalogPtr queryCatalog;
    RequestQueuePtr queryRequestQueue;
    Optimizer::SemanticQueryValidationPtr semanticQueryValidation;
    Optimizer::SyntacticQueryValidationPtr syntacticQueryValidation;
    bool enableSemanticQueryValidation;

    void assignQueryAndOperatorIds(QueryPlanPtr queryPlan);
};

};// namespace NES

#endif  // NES_INCLUDE_SERVICES_QUERYSERVICE_HPP_

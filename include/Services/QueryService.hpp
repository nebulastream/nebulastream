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

#ifndef QUERYSERVICE_HPP
#define QUERYSERVICE_HPP

#include <API/Query.hpp>
#include <Plans/Query/QueryId.hpp>
#include <cpprest/json.h>

namespace NES::Optimizer {
class SyntacticQueryValidation;
typedef std::shared_ptr<SyntacticQueryValidation> SyntacticQueryValidationPtr;

class SemanticQueryValidation;
typedef std::shared_ptr<SemanticQueryValidation> SemanticQueryValidationPtr;
}// namespace NES::Optimizer

namespace NES {

class QueryService;
typedef std::shared_ptr<QueryService> QueryServicePtr;

class QueryCatalog;
typedef std::shared_ptr<QueryCatalog> QueryCatalogPtr;

class NESRequestQueue;
typedef std::shared_ptr<NESRequestQueue> NESRequestQueuePtr;

/**
 * @brief: This class is responsible for handling requests related to submitting, fetching information, and deleting different queries.
 */
class QueryService {

  public:
    explicit QueryService(QueryCatalogPtr queryCatalog, NESRequestQueuePtr queryRequestQueue, StreamCatalogPtr streamCatalog,
                          bool enableSemanticQueryValidation);

    ~QueryService();

    /**
     * Register the incoming query in the system by add it to the scheduling queue for further processing, and return the query Id assigned.
     * @param queryString : query in string form.
     * @param placementStrategyName : name of the placement strategy to be used.
     * @return queryId : query id of the valid input query.
     * @throws InvalidQueryException : when query string is not valid.
     * @throws InvalidArgumentException : when the placement strategy is not valid.
     */
    uint64_t validateAndQueueAddRequest(std::string queryString, std::string placementStrategyName);

    /**
     * @deprecated NOT TO BE USED
     * @brief This method is used for submitting the queries directly to the system.
     * @param queryString : Query string
     * @param queryPtr : Query Object
     * @param placementStrategyName : Name of the placement strategy
     * @return query id
     */
    uint64_t addQueryRequest(std::string queryString, QueryPtr queryPtr, std::string placementStrategyName);

    /**
     * @brief This method calls addQueryRequest with an empty string.
     * @param queryPtr : Query Object
     * @param placementStrategyName : Name of the placement strategy
     * @return query id
     */
    uint64_t addQueryRequest(QueryPtr queryPtr, std::string placementStrategyName);

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
    NESRequestQueuePtr queryRequestQueue;
    Optimizer::SemanticQueryValidationPtr semanticQueryValidation;
    Optimizer::SyntacticQueryValidationPtr syntacticQueryValidation;
    bool enableSemanticQueryValidation;
};

};// namespace NES

#endif//QUERYSERVICE_HPP

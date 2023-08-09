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

#ifndef NES_CORE_INCLUDE_SERVICES_QUERYSERVICE_HPP_
#define NES_CORE_INCLUDE_SERVICES_QUERYSERVICE_HPP_

#include <API/Query.hpp>
#include <Common/Identifiers.hpp>
#include <Configurations/Coordinator/OptimizerConfiguration.hpp>
#include <future>

namespace NES::Optimizer {
class SyntacticQueryValidation;
using SyntacticQueryValidationPtr = std::shared_ptr<SyntacticQueryValidation>;

class SemanticQueryValidation;
using SemanticQueryValidationPtr = std::shared_ptr<SemanticQueryValidation>;
}// namespace NES::Optimizer

namespace NES {

class QueryService;
using QueryServicePtr = std::shared_ptr<QueryService>;

class QueryCatalogService;
using QueryCatalogServicePtr = std::shared_ptr<QueryCatalogService>;

class RequestQueue;
using RequestQueuePtr = std::shared_ptr<RequestQueue>;

class QueryParsingService;
using QueryParsingServicePtr = std::shared_ptr<QueryParsingService>;

namespace Catalogs {
namespace Source {
class SourceCatalog;
using SourceCatalogPtr = std::shared_ptr<SourceCatalog>;
}// namespace Source

namespace UDF {
class UDFCatalog;
using UDFCatalogPtr = std::shared_ptr<UDFCatalog>;
}// namespace UDF
}// namespace Catalogs

class StorageHandler;

namespace Experimental {
template<typename StorageHandler>
class AsyncRequestExecutor;
using AsyncRequestExecutorPtr = std::shared_ptr<AsyncRequestExecutor<StorageHandler>>;
}// namespace Experimental

/**
 * @brief: This class is responsible for handling requests related to submitting, fetching information, and deleting different queryIdAndCatalogEntryMapping.
 */
class QueryService {

  public:
    explicit QueryService(const QueryCatalogServicePtr& queryCatalogService,
                          const RequestQueuePtr& queryRequestQueue,
                          const Catalogs::Source::SourceCatalogPtr& sourceCatalog,
                          const QueryParsingServicePtr& queryParsingService,
                          const Catalogs::UDF::UDFCatalogPtr& udfCatalog,
                          bool useNewRequestExecutor,
                          Configurations::OptimizerConfiguration optimizerConfiguration,
                          const Experimental::AsyncRequestExecutorPtr& asyncRequestExecutor);

    /**
     * @brief Register the incoming query in the system by add it to the scheduling queue for further processing, and return the query Id assigned.
     * @param queryString : query in string form.
     * @param placementStrategyName : name of the placement strategy to be used.
     * @param faultTolerance : fault-tolerance guarantee for the given query.
     * @param lineage : lineage type for the given query.
     * @return queryId : query id of the valid input query.
     * @throws InvalidQueryException : when query string is not valid.
     * @throws InvalidArgumentException : when the placement strategy is not valid.
     */
    QueryId validateAndQueueAddQueryRequest(const std::string& queryString,
                                            const std::string& placementStrategyName,
                                            const FaultToleranceType faultTolerance = FaultToleranceType::NONE,
                                            const LineageType lineage = LineageType::NONE);

    /**
     * @brief Register the incoming query in the system by add it to the scheduling queue for further processing, and return the query Id assigned.
     * @param queryString : queryIdAndCatalogEntryMapping in string format
     * @param queryPlan : Query Plan Pointer Object
     * @param placementStrategyName : Name of the placement strategy
     * @param faultTolerance : fault-tolerance guarantee for the given query.
     * @param lineage : lineage type for the given query.
     * @return query id
     */
    QueryId addQueryRequest(const std::string& queryString,
                            const QueryPlanPtr& queryPlan,
                            const std::string& placementStrategyName,
                            const FaultToleranceType faultTolerance = FaultToleranceType::NONE,
                            const LineageType lineage = LineageType::NONE);

    /**
     * Register the incoming stop query request in the system by add it to the scheduling queue for further processing.
     * @param queryId : query id of the query to be stopped.
     * @returns: true if successful
     * @throws QueryNotFoundException : when query id is not found in the query catalog.
     * @throws InvalidQueryStateException : when the query is found to be in an invalid state.
     */
    bool validateAndQueueStopQueryRequest(QueryId queryId);

    /**
     * Register the request to fail shared query plan.
     *
     * @warning: this method is primarily designed to be called only by the system.
     *
     * @param sharedQueryId : shared query plan id of the shared query plan to be stopped.
     * @param failureReason : reason for shared query plan failure.
     * @returns: true if successful
     */
    bool validateAndQueueFailQueryRequest(SharedQueryId sharedQueryId, const std::string& failureReason);

  private:
    /**
     * Assign unique operator ids to the incoming query plan from a client.
     * @param queryPlan : query plan to process
     */
    void assignOperatorIds(QueryPlanPtr queryPlan);

    QueryCatalogServicePtr queryCatalogService;
    RequestQueuePtr queryRequestQueue;
    Optimizer::SemanticQueryValidationPtr semanticQueryValidation;
    Optimizer::SyntacticQueryValidationPtr syntacticQueryValidation;
    Configurations::OptimizerConfiguration optimizerConfiguration;
    bool useNewRequestExecutor;
    Experimental::AsyncRequestExecutorPtr asyncRequestExecutor;
};

};// namespace NES

#endif// NES_CORE_INCLUDE_SERVICES_QUERYSERVICE_HPP_

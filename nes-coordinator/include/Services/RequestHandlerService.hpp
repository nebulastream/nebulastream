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

#ifndef NES_COORDINATOR_INCLUDE_SERVICES_REQUESTHANDLERSERVICE_HPP_
#define NES_COORDINATOR_INCLUDE_SERVICES_REQUESTHANDLERSERVICE_HPP_

#include <Configurations/Coordinator/OptimizerConfiguration.hpp>
#include <Identifiers/Identifiers.hpp>
#include <StatisticCollection/QueryGeneration/StatisticIdsExtractor.hpp>
#include <Statistics/StatisticKey.hpp>
#include <Util/Placement/PlacementStrategy.hpp>
#include <future>
#include <nlohmann/json.hpp>

namespace z3 {
class context;
using ContextPtr = std::shared_ptr<context>;
}// namespace z3

namespace NES {

namespace Windowing {
class WindowType;
using WindowTypePtr = std::shared_ptr<WindowType>;
}// namespace Windowing

namespace Statistic {
class Characteristic;
class TriggerCondition;
class SendingPolicy;
class AbstractStatisticQueryGenerator;
class StatisticRegistry;

using CharacteristicPtr = std::shared_ptr<Characteristic>;
using TriggerConditionPtr = std::shared_ptr<TriggerCondition>;
using SendingPolicyPtr = std::shared_ptr<SendingPolicy>;
using AbstractStatisticQueryGeneratorPtr = std::shared_ptr<AbstractStatisticQueryGenerator>;
using StatisticRegistryPtr = std::shared_ptr<StatisticRegistry>;
}// namespace Statistic

namespace Optimizer {
class SyntacticQueryValidation;
using SyntacticQueryValidationPtr = std::shared_ptr<SyntacticQueryValidation>;

class SemanticQueryValidation;
using SemanticQueryValidationPtr = std::shared_ptr<SemanticQueryValidation>;

class PlacementAmendmentHandler;
using PlacementAmendmentHandlerPtr = std::shared_ptr<PlacementAmendmentHandler>;
}// namespace Optimizer

class Schema;
using SchemaPtr = std::shared_ptr<Schema>;

class QueryPlan;
using QueryPlanPtr = std::shared_ptr<QueryPlan>;

class RequestHandlerService;
using RequestHandlerServicePtr = std::shared_ptr<RequestHandlerService>;

class QueryParsingService;
using QueryParsingServicePtr = std::shared_ptr<QueryParsingService>;

class TopologyLinkInformation;

namespace Catalogs {
namespace Source {
class SourceCatalog;
using SourceCatalogPtr = std::shared_ptr<SourceCatalog>;
}// namespace Source

namespace UDF {
class UDFCatalog;
using UDFCatalogPtr = std::shared_ptr<UDFCatalog>;
}// namespace UDF

namespace Query {
class QueryCatalog;
using QueryCatalogPtr = std::shared_ptr<QueryCatalog>;
}// namespace Query
}// namespace Catalogs

namespace RequestProcessor {
class AsyncRequestProcessor;
using AsyncRequestProcessorPtr = std::shared_ptr<AsyncRequestProcessor>;

struct ISQPRequestResponse;
using ISQPRequestResponsePtr = std::shared_ptr<ISQPRequestResponse>;

class ISQPEvent;
using ISQPEventPtr = std::shared_ptr<ISQPEvent>;

class BaseUpdateSourceCatalogEvent;
using UpdateSourceCatalogEventPtr = std::shared_ptr<BaseUpdateSourceCatalogEvent>;

class AddPhysicalSourcesEvent;
using AddPhysicalSourcesEventPtr = std::shared_ptr<AddPhysicalSourcesEvent>;

struct PhysicalSourceDefinition;

}// namespace RequestProcessor

/**
 * @brief: This class is responsible for handling requests related to submitting, fetching information, and deleting different queryIdAndCatalogEntryMapping,
 * as well as modifying the topology.
 */
class RequestHandlerService {

  public:
    explicit RequestHandlerService(Configurations::OptimizerConfiguration optimizerConfiguration,
                                   const QueryParsingServicePtr& queryParsingService,
                                   const Catalogs::Query::QueryCatalogPtr& queryCatalog,
                                   const Catalogs::Source::SourceCatalogPtr& sourceCatalog,
                                   const Catalogs::UDF::UDFCatalogPtr& udfCatalog,
                                   const NES::RequestProcessor::AsyncRequestProcessorPtr& asyncRequestExecutor,
                                   const z3::ContextPtr& z3Context,
                                   const Statistic::AbstractStatisticQueryGeneratorPtr& statisticQueryGenerator,
                                   const Statistic::StatisticRegistryPtr& statisticRegistry,
                                   const Optimizer::PlacementAmendmentHandlerPtr& placementAmendmentHandler);

    /**
     * @brief Register the incoming query in the system by add it to the scheduling queue for further processing, and return the query Id assigned.
     * @param queryString : query in string form.
     * @param placementStrategy : name of the placement strategy to be used.
     * @return queryId : query id of the valid input query.
     * @throws InvalidQueryException : when query string is not valid.
     * @throws InvalidArgumentException : when the placement strategy is not valid.
     */
    QueryId validateAndQueueAddQueryRequest(const std::string& queryString, const Optimizer::PlacementStrategy placementStrategy);

    /**
     * @brief Register the incoming query in the system by add it to the scheduling queue for further processing, and return the query Id assigned.
     * @param queryPlan : Query Plan Pointer Object
     * @param placementStrategy : Name of the placement strategy
     * @return query id
     */
    QueryId validateAndQueueAddQueryRequest(const QueryPlanPtr& queryPlan, const Optimizer::PlacementStrategy placementStrategy);

    /**
     * @brief Register the incoming query in the system by add it to the scheduling queue for further processing, and return the query Id assigned.
     * @param queryPlan : Query Plan Pointer Object
     * @param placementStrategy : Name of the placement strategy
     * @param faultTolerance : fault-tolerance guarantee for the given query.
     * @param lineage : lineage type for the given query.
     * @return query id
     */
    nlohmann::json validateAndQueueExplainQueryRequest(const QueryPlanPtr& queryPlan,
                                                       const Optimizer::PlacementStrategy placementStrategy);

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
     * @param decomposedQueryId: id of the subquery plan that failed
     * @param decomposedQueryVersion: version of the subquery plan that failed
     * @param failureReason : reason for shared query plan failure.
     * @returns: true if successful
     */
    bool validateAndQueueFailQueryRequest(SharedQueryId sharedQueryId,
                                          DecomposedQueryId decomposedQueryId,
                                          DecomposedQueryPlanVersion decomposedQueryVersion,
                                          const std::string& failureReason);

    /**
     * @brief Process multiple query and topology change request represented by isqp events in a batch
     * @param isqpEvents a vector of ISQP requests to be handled
     * @return response to the execution of the request
     */
    RequestProcessor::ISQPRequestResponsePtr queueISQPRequest(const std::vector<RequestProcessor::ISQPEventPtr>& isqpEvents, bool waitForResponse = true, uint64_t count = 0);

    /**
     * @brief Processes a track requests by mapping it to a statistic query and returning the statistic keys
     * @param characteristic: What type of the statistic is being tracked, i.e, data, infrastructure, or workload
     * @param window: Over what window to track the statistics over
     * @param triggerCondition: Condition to trigger the callback function. Is being called, if a statistic is created
     * @param sendingPolicy: Policy to send the data to the statistic sink
     * @param callBack: Function that should be called, if triggerCondition evaluates to true
     * @return: Vector of StatisticKeys
     */
    std::vector<Statistic::StatisticKey> trackStatisticRequest(const Statistic::CharacteristicPtr& characteristic,
                                                               const Windowing::WindowTypePtr& window,
                                                               const Statistic::TriggerConditionPtr& triggerCondition,
                                                               const Statistic::SendingPolicyPtr& sendingPolicy,
                                                               std::function<void(Statistic::CharacteristicPtr)> callBack);

    /**
     * @brief Processes a track requests by mapping it to a statistic query and returning the statistic keys
     * @param characteristic: What type of the statistic is being tracked, i.e, data, infrastructure, or workload
     * @param window: Over what window to track the statistics over
     * @return Vector of StatisticKeys
     */
    std::vector<Statistic::StatisticKey> trackStatisticRequest(const Statistic::CharacteristicPtr& characteristic,
                                                               const Windowing::WindowTypePtr& window);

    /**
      * @brief register one or multiple new physical sources
      * @param additions a vector of physical source additions
      * @param workerId the id of the worker hosting the physical source
      * @return true on success
      */
    bool queueRegisterPhysicalSourceRequest(std::vector<RequestProcessor::PhysicalSourceDefinition> additions,
                                            WorkerId workerId) const;

    /**
      * @brief register a new logical source
      * @param logicalSourceName the name of the logical source
      * @param schema the schema of the logical source
      * @return true on success
      */
    bool queueRegisterLogicalSourceRequest(const std::string& logicalSourceName, SchemaPtr schema) const;

    /**
      * @brief unregister an existing physical source
      * @param physicalSourceName the name of the physical source to unregister
      * @param logicalSourceName the name of the logical source to which the physical source belongs
      * @param workerId the id of the worker hosting the physical source
      * @return true on success
      */
    bool queueUnregisterPhysicalSourceRequest(const std::string& physicalSourceName,
                                              const std::string& logicalSourceName,
                                              WorkerId workerId) const;

    /**
      * @brief unregister all existing physical sources
      * @param additions a vector of physical source additions
      * @param workerId the id of the worker hosting the physical source
      * @return true on success
      */
    bool queueUnregisterAllPhysicalSourcesByWorkerRequest(WorkerId workerId) const;

    /**
      * @brief add key distribution entry
      * @param logicalSourceName the name of the logical source to which the physical source belongs
      * @param physicalSourceName the name of the physical source to unregister
      * @param workerId the id of the worker hosting the physical source
      * @param value the statistics from request
      * @return true on success
      */
    bool queueAddKeyDistributionEntryRequest(const std::string& logicalSourceName,
                                             const std::string& physicalSourceName,
                                             WorkerId workerId,
                                             const std::string& value) const;

    /**
      * @brief unregister an existing logical source
      * @param logicalSourceName the name of the logical source
      * @return true on success
      */
    bool queueUnregisterLogicalSourceRequest(const std::string& logicalSourceName) const;

    /**
      * @brief update an existing logical source
      * @param logicalSourceName the name of the logical source
      * @param schema the new schema of the logical source
      * @return true on success
      */
    bool queueUpdateLogicalSourceRequest(const std::string& logicalSourceName, SchemaPtr schema) const;

    /**
     * @brief get all logical sources
     * @return json object containing all logical sources
     */
    nlohmann::json queueGetAllLogicalSourcesRequest() const;

    /**
     * @brief get all physical sources belonging to a logical source
     * @return json object containing the sources
     */
    nlohmann::json queueGetPhysicalSourcesRequest(std::string logicelSourceName) const;

    /**
     * @brief get the schema of a logical source
     * @return json object containing the schema
     */
    SchemaPtr queueGetLogicalSourceSchemaRequest(std::string logicelSourceName) const;

    bool isIncrementalPlacementEnabled();

    void validateAndQueueMultiQueryRequest(std::vector<std::pair<std::string, Optimizer::PlacementStrategy>> queryStrings, uint64_t duplicationFactor = 1);

    void validateAndQueueMultiQueryRequestParallel(std::vector<std::string> queries);
  private:
    /**
     * @brief helper function to create a request to modify the query catalog
     * @param sourceActions a vector containing the modifications to make to the catalog
     * @return true on successfull execution of the request
     */
    bool handleCatalogUpdateRequest(RequestProcessor::UpdateSourceCatalogEventPtr event) const;

    /**
     * Assign unique operator ids to the incoming query plan from a client.
     * @param queryPlan : query plan to process
     */
    void assignOperatorIds(QueryPlanPtr queryPlan);

    Configurations::OptimizerConfiguration optimizerConfiguration;
    QueryParsingServicePtr queryParsingService;
    Catalogs::Query::QueryCatalogPtr queryCatalog;
    Optimizer::SemanticQueryValidationPtr semanticQueryValidation;
    Optimizer::SyntacticQueryValidationPtr syntacticQueryValidation;
    NES::RequestProcessor::AsyncRequestProcessorPtr asyncRequestExecutor;
    z3::ContextPtr z3Context;
    Statistic::AbstractStatisticQueryGeneratorPtr statisticQueryGenerator;
    Statistic::StatisticRegistryPtr statisticRegistry;
    Statistic::StatisticIdsExtractor statisticIdsExtractor;
    Optimizer::PlacementAmendmentHandlerPtr placementAmendmentHandler;
    void validateAndQueueMultiQueryRequest(std::vector<std::string, Optimizer::PlacementStrategy> queryStrings,
                                           uint64_t duplicationFactor);
};

}// namespace NES

#endif// NES_COORDINATOR_INCLUDE_SERVICES_REQUESTHANDLERSERVICE_HPP_

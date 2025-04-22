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

#ifndef NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_REQUESTTYPES_ADDQUERYREQUEST_HPP_
#define NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_REQUESTTYPES_ADDQUERYREQUEST_HPP_

#include <Identifiers/Identifiers.hpp>
#include <RequestProcessor/RequestTypes/AbstractUniRequest.hpp>
#include <Util/CheckpointStorageType.hpp>
#include <Util/FaultToleranceType.hpp>

namespace z3 {
class context;
using ContextPtr = std::shared_ptr<context>;
}// namespace z3

namespace NES {

class GlobalQueryPlan;
using GlobalQueryPlanPtr = std::shared_ptr<GlobalQueryPlan>;

namespace Optimizer {
class GlobalExecutionPlan;
using GlobalExecutionPlanPtr = std::shared_ptr<GlobalExecutionPlan>;
}// namespace Optimizer

class Topology;
using TopologyPtr = std::shared_ptr<Topology>;

class WorkerRPCClient;
using WorkerRPCClientPtr = std::shared_ptr<WorkerRPCClient>;

class QueryParsingService;
using QueryParsingServicePtr = std::shared_ptr<QueryParsingService>;

namespace Configurations {
class CoordinatorConfiguration;
using CoordinatorConfigurationPtr = std::shared_ptr<CoordinatorConfiguration>;
}// namespace Configurations

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

namespace Optimizer {

class TypeInferencePhase;
using TypeInferencePhasePtr = std::shared_ptr<TypeInferencePhase>;

class QueryRewritePhase;
using QueryRewritePhasePtr = std::shared_ptr<QueryRewritePhase>;

class SampleCodeGenerationPhase;
using SampleCodeGenerationPhasePtr = std::shared_ptr<SampleCodeGenerationPhase>;

class OriginIdInferencePhase;
using OriginIdInferencePhasePtr = std::shared_ptr<OriginIdInferencePhase>;

class TopologySpecificQueryRewritePhase;
using TopologySpecificQueryRewritePhasePtr = std::shared_ptr<TopologySpecificQueryRewritePhase>;

class SignatureInferencePhase;
using SignatureInferencePhasePtr = std::shared_ptr<SignatureInferencePhase>;

class QueryMergerPhase;
using QueryMergerPhasePtr = std::shared_ptr<QueryMergerPhase>;

class MemoryLayoutSelectionPhase;
using MemoryLayoutSelectionPhasePtr = std::shared_ptr<MemoryLayoutSelectionPhase>;

class TypeInferencePhase;
using TypeInferencePhasePtr = std::shared_ptr<TypeInferencePhase>;

class QueryRewritePhase;
using QueryRewritePhasePtr = std::shared_ptr<QueryRewritePhase>;

class QueryPlacementAmendmentPhase;
using QueryPlacementAmendmentPhasePtr = std::shared_ptr<QueryPlacementAmendmentPhase>;

class GlobalQueryPlanUpdatePhase;
using GlobalQueryPlanUpdatePhasePtr = std::shared_ptr<GlobalQueryPlanUpdatePhase>;

class PlacementAmendmentHandler;
using PlacementAmendmentHandlerPtr = std::shared_ptr<PlacementAmendmentHandler>;
}// namespace Optimizer

namespace RequestProcessor {

//a response to the creator of the request
struct AddQueryResponse : public AbstractRequestResponse {
    explicit AddQueryResponse(QueryId queryId) : queryId(queryId){};
    QueryId queryId;
};

class AddQueryRequest;
using AddQueryRequestPtr = std::shared_ptr<AddQueryRequest>;

class AddQueryRequest : public AbstractUniRequest {
  public:
    /**
     * @brief Constructor
     * @param queryString: the query string
     * @param queryPlacementStrategy: the placement strategy
     * @param maxRetries: Maximum number of retry attempts for the request
     * @param z3Context: The z3 context to be used for the request, needed for query merging phase
     * @param queryParsingService: parsing string queries
     * @param placementAmendmentHandler: the placement amendment handler
     * @param faultToleranceType: fault tolerance type
     * @param checkpointStorageType: storage option if checkpointing enabled
     */
    AddQueryRequest(const std::string& queryString,
                    const Optimizer::PlacementStrategy queryPlacementStrategy,
                    const uint8_t maxRetries,
                    const z3::ContextPtr& z3Context,
                    const QueryParsingServicePtr& queryParsingService,
                    const Optimizer::PlacementAmendmentHandlerPtr& placementAmendmentHandler,
                    FaultToleranceType faultTolerance,
                    CheckpointStorageType checkpointStorage);

    /**
     * @brief Constructor
     * @param queryPlan: the query plan
     * @param queryPlacementStrategy: the placement strategy
     * @param maxRetries: Maximum number of retry attempts for the request
     * @param z3Context: The z3 context to be used for the request, needed for query merging phase
     * @param placementAmendmentHandler: the placement amendment handler
     * @param faultToleranceType: fault tolerance type
     * @param checkpointStorageType: storage option if checkpointing enabled
     */
    AddQueryRequest(const QueryPlanPtr& queryPlan,
                    const Optimizer::PlacementStrategy queryPlacementStrategy,
                    const uint8_t maxRetries,
                    const z3::ContextPtr& z3Context,
                    const Optimizer::PlacementAmendmentHandlerPtr& placementAmendmentHandler,
                    FaultToleranceType faultTolerance,
                    CheckpointStorageType checkpointStorage);

    /**
     * @brief creates a new AddQueryRequest object
     * @param queryPlan: the query plan
     * @param queryPlacementStrategy: the placement strategy
     * @param maxRetries: Maximum number of retry attempts for the request
     * @param z3Context: The z3 context to be used for the request, needed for query merging phase
     * @param placementAmendmentHandler: the placement amendment handler
     * @param faultToleranceType: fault tolerance type
     * @param checkpointStorageType: storage option if checkpointing enabled
     */
    static AddQueryRequestPtr create(const QueryPlanPtr& queryPlan,
                                     const Optimizer::PlacementStrategy queryPlacementStrategy,
                                     const uint8_t maxRetries,
                                     const z3::ContextPtr& z3Context,
                                     const Optimizer::PlacementAmendmentHandlerPtr& placementAmendmentHandler,
                                     FaultToleranceType faultTolerance = FaultToleranceType::NONE,
                                     CheckpointStorageType checkpointStorage = CheckpointStorageType::NONE);

    /**
     * @brief creates a new AddQueryRequest object
     * @param queryPlan: the query plan
     * @param queryPlacementStrategy: the placement strategy
     * @param maxRetries: Maximum number of retry attempts for the request
     * @param z3Context: The z3 context to be used for the request, needed for query merging phase
     * @param queryParsingService: parsing string query
     * @param placementAmendmentHandler: the placement amendment handler
     * @param faultToleranceType: fault tolerance type
     * @param checkpointStorageType: storage option if checkpointing enabled
     */
    static AddQueryRequestPtr create(const std::string& queryPlan,
                                     const Optimizer::PlacementStrategy queryPlacementStrategy,
                                     const uint8_t maxRetries,
                                     const z3::ContextPtr& z3Context,
                                     const QueryParsingServicePtr& queryParsingService,
                                     const Optimizer::PlacementAmendmentHandlerPtr& placementAmendmentHandler,
                                     FaultToleranceType faultTolerance = FaultToleranceType::NONE,
                                     CheckpointStorageType checkpointStorage = CheckpointStorageType::NONE);

  protected:
    /**
     * @brief Performs request specific error handling to be done before changes to the storage are rolled back
     * @param ex: The exception encountered
     * @param storageHandle: The storage access handle used by the request
     */
    void preRollbackHandle(std::exception_ptr ex, const StorageHandlerPtr& storageHandler) override;

    /**
     * @brief Roll back any changes made by a request that did not complete due to errors.
     * @param ex: The exception thrown during request execution.
     * @param storageHandle: The storage access handle that was used by the request to modify the system state.
     * @return a list of follow up requests to be executed (can be empty if no further actions are required)
     */
    std::vector<AbstractRequestPtr> rollBack(std::exception_ptr ex, const StorageHandlerPtr& storageHandle) override;

    /**
     * @brief Performs request specific error handling to be done after changes to the storage are rolled back
     * @param ex: The exception encountered
     * @param storageHandle: The storage access handle used by the request
     */
    void postRollbackHandle(std::exception_ptr ex, const StorageHandlerPtr& storageHandler) override;

    /**
     * @brief Executes the request logic.
     * @param storageHandle: a handle to access the coordinators data structures which might be needed for executing the
     * request
     * @return a list of follow up requests to be executed (can be empty if no further actions are required)
     */
    std::vector<AbstractRequestPtr> executeRequestLogic(const StorageHandlerPtr& storageHandler) override;

    /**
     * @brief Assign new operator ids to the input query plan
     * @param queryPlan : the input query plan
     */
    void assignOperatorIds(const QueryPlanPtr& queryPlan);

  private:
    QueryId queryId;
    std::string queryString;
    QueryPlanPtr queryPlan;
    Optimizer::PlacementStrategy queryPlacementStrategy;
    z3::ContextPtr z3Context;
    QueryParsingServicePtr queryParsingService;
    Optimizer::PlacementAmendmentHandlerPtr placementAmendmentHandler;
    FaultToleranceType faultTolerance;
    CheckpointStorageType checkpointStorage;

    void markAsFailedInQueryCatalog(std::exception& e, const StorageHandlerPtr& storageHandler);
    void removeFromGlobalQueryPlanAndMarkAsFailed(std::exception& e, const StorageHandlerPtr& storageHandler);
};
}// namespace RequestProcessor
}// namespace NES
#endif// NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_REQUESTTYPES_ADDQUERYREQUEST_HPP_

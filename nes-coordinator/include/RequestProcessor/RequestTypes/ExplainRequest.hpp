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

#ifndef NES_EXPLAINREQUEST_HPP
#define NES_EXPLAINREQUEST_HPP

#include <Common/Identifiers.hpp>
#include <RequestProcessor/RequestTypes/AbstractRequest.hpp>
#include <nlohmann/json.hpp>

namespace NES {

class GlobalQueryPlan;
using GlobalQueryPlanPtr = std::shared_ptr<GlobalQueryPlan>;

class QueryCatalogService;
using QueryCatalogServicePtr = std::shared_ptr<QueryCatalogService>;

class GlobalExecutionPlan;
using GlobalExecutionPlanPtr = std::shared_ptr<GlobalExecutionPlan>;

class Topology;
using TopologyPtr = std::shared_ptr<Topology>;

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

class QueryPlacementPhase;
using QueryPlacementPhasePtr = std::shared_ptr<QueryPlacementPhase>;

class GlobalQueryPlanUpdatePhase;
using GlobalQueryPlanUpdatePhasePtr = std::shared_ptr<GlobalQueryPlanUpdatePhase>;
}// namespace Optimizer

namespace RequestProcessor::Experimental {

//a response to the creator of the request
struct ExplainResponse : public AbstractRequestResponse {
    explicit ExplainResponse(nlohmann::json jsonResponse) : jsonResponse(jsonResponse){};
    nlohmann::json jsonResponse;
};

class ExplainRequest;
using ExplainRequestPtr = std::shared_ptr<ExplainRequest>;

class ExplainRequest : public AbstractRequest {
  public:

    /**
     * @brief Constructor
     * @param queryPlan: the query plan
     * @param queryPlacementStrategy: the placement strategy
     * @param faultTolerance: the fault tolerance aspect of the query
     * @param lineage: the lineage property of the query
     * @param maxRetries: Maximum number of retry attempts for the request
     * @param z3Context: The z3 context to be used for the request, needed for query merging phase
     */
    ExplainRequest(const QueryPlanPtr& queryPlan,
                    const Optimizer::PlacementStrategy queryPlacementStrategy,
                    const FaultToleranceType faultTolerance,
                    const LineageType lineage,
                    const uint8_t maxRetries,
                    const z3::ContextPtr& z3Context);

    /**
     * @brief creates a new AddQueryRequest object
     * @param queryPlan: the query plan
     * @param queryPlacementStrategy: the placement strategy
     * @param faultTolerance: the fault tolerance aspect of the query
     * @param lineage: the lineage property of the query
     * @param maxRetries: Maximum number of retry attempts for the request
     * @param z3Context: The z3 context to be used for the request, needed for query merging phase
     */
    static ExplainRequestPtr create(const QueryPlanPtr& queryPlan,
                                     const Optimizer::PlacementStrategy queryPlacementStrategy,
                                     const FaultToleranceType faultTolerance,
                                     const LineageType lineage,
                                     const uint8_t maxRetries,
                                     const z3::ContextPtr& z3Context);

  protected:
    /**
     * @brief Performs request specific error handling to be done before changes to the storage are rolled back
     * @param ex: The exception encountered
     * @param storageHandle: The storage access handle used by the request
     */
    void preRollbackHandle(const RequestExecutionException& ex, const StorageHandlerPtr& storageHandler) override;

    /**
     * @brief Roll back any changes made by a request that did not complete due to errors.
     * @param ex: The exception thrown during request execution.
     * @param storageHandle: The storage access handle that was used by the request to modify the system state.
     * @return a list of follow up requests to be executed (can be empty if no further actions are required)
     */
    std::vector<AbstractRequestPtr> rollBack(RequestExecutionException& ex, const StorageHandlerPtr& storageHandle) override;

    /**
     * @brief Performs request specific error handling to be done after changes to the storage are rolled back
     * @param ex: The exception encountered
     * @param storageHandle: The storage access handle used by the request
     */
    void postRollbackHandle(const RequestExecutionException& ex, const StorageHandlerPtr& storageHandler) override;

    /**
     * @brief Performs steps to be done after execution of the request logic, e.g. unlocking the required data structures
     * @param storageHandle: The storage access handle used by the request
     * @param requiredResources: The resources required during the execution phase
     */
    void postExecution(const StorageHandlerPtr& storageHandler) override;

    /**
     * @brief Executes the request logic.
     * @param storageHandle: a handle to access the coordinators data structures which might be needed for executing the
     * request
     * @return a list of follow up requests to be executed (can be empty if no further actions are required)
     */
    std::vector<AbstractRequestPtr> executeRequestLogic(const StorageHandlerPtr& storageHandler) override;

  private:
    QueryId queryId;
    std::string queryString;
    QueryPlanPtr queryPlan;
    Optimizer::PlacementStrategy queryPlacementStrategy;
    FaultToleranceType faultTolerance;
    LineageType lineage;
    z3::ContextPtr z3Context;
    QueryParsingServicePtr queryParsingService;
};
}// namespace RequestProcessor::Experimental
}// namespace NES

#endif//NES_EXPLAINREQUEST_HPP

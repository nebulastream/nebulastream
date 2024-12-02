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

#ifndef NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_REQUESTTYPES_EXPLAINREQUEST_HPP_
#define NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_REQUESTTYPES_EXPLAINREQUEST_HPP_

#include <Identifiers/Identifiers.hpp>
#include <RequestProcessor/RequestTypes/AbstractUniRequest.hpp>
#include <nlohmann/json.hpp>

namespace z3 {
class context;
using ContextPtr = std::shared_ptr<context>;
}// namespace z3

namespace NES {

class GlobalQueryPlan;
using GlobalQueryPlanPtr = std::shared_ptr<GlobalQueryPlan>;

class DecomposedQueryPlan;
using DecomposedQueryPlanPtr = std::shared_ptr<DecomposedQueryPlan>;

class QueryCatalogService;
using QueryCatalogServicePtr = std::shared_ptr<QueryCatalogService>;

class Topology;
using TopologyPtr = std::shared_ptr<Topology>;

class TopologyNode;
using TopologyNodePtr = std::shared_ptr<TopologyNode>;

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

class GlobalExecutionPlan;
using GlobalExecutionPlanPtr = std::shared_ptr<GlobalExecutionPlan>;
}// namespace Optimizer

namespace RequestProcessor {

//a response to the creator of the request
struct ExplainResponse : public AbstractRequestResponse {
    explicit ExplainResponse(nlohmann::json jsonResponse) : jsonResponse(jsonResponse){};
    nlohmann::json jsonResponse;
};

class ExplainRequest;
using ExplainRequestPtr = std::shared_ptr<ExplainRequest>;

class ExplainRequest : public AbstractUniRequest {
  public:
    /**
     * @brief Constructor
     * @param queryPlan: the query plan
     * @param queryPlacementStrategy: the placement strategy
     * @param maxRetries: Maximum number of retry attempts for the request
     * @param z3Context: The z3 context to be used for the request, needed for query merging phase
     */
    ExplainRequest(const QueryPlanPtr& queryPlan,
                   const Optimizer::PlacementStrategy queryPlacementStrategy,
                   const FaultToleranceType faultToleranceType,
                   const uint8_t maxRetries,
                   const z3::ContextPtr& z3Context);

    /**
     * @brief creates a new AddQueryRequest object
     * @param queryPlan: the query plan
     * @param queryPlacementStrategy: the placement strategy
     * @param maxRetries: Maximum number of retry attempts for the request
     * @param z3Context: The z3 context to be used for the request, needed for query merging phase
     */
    static ExplainRequestPtr create(const QueryPlanPtr& queryPlan,
                                    const Optimizer::PlacementStrategy queryPlacementStrategy,
                                    const FaultToleranceType faultToleranceType,
                                    const uint8_t maxRetries,
                                    const z3::ContextPtr& z3Context);

  protected:
    /**
     * @brief Performs request specific error handling to be done before changes to the storage are rolled back
     * @param ex: The exception encountered
     * @param storageHandler: The storage access handle used by the request
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
     * @param storageHandler: The storage access handle used by the request
     */
    void postRollbackHandle(std::exception_ptr ex, const StorageHandlerPtr& storageHandler) override;

    /**
     * @brief Executes the request logic.
     * @param storageHandler: a handle to access the coordinators data structures which might be needed for executing the
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
    /**
     * @brief Add opencl acceleration code to the query plan
     * @param accelerationServiceURL: acceleration service url
     * @param decomposedQueryPlan : the query plan
     * @param topologyNode : the topology node
     */
    void addOpenCLAccelerationCode(const std::string& accelerationServiceURL,
                                   const DecomposedQueryPlanPtr& decomposedQueryPlan,
                                   const TopologyNodePtr& topologyNode);

    /**
     * @brief create json from the execution plan
     * @param SharedQueryId : the shared query id
     * @param globalExecutionPlan : the global execution plan
     * @param topology : topology pointer
     * @param accelerateJavaUDFs : accelerate java udfs
     * @param accelerationServiceURL: url for fetching acceleration code
     * @param sampleCodeGenerationPhase> phase used to generate sample code
     * @return json representing the global execution plan
     */
    nlohmann::json getExecutionPlanForSharedQueryAsJson(SharedQueryId sharedQueryId,
                                                        const Optimizer::GlobalExecutionPlanPtr& globalExecutionPlan,
                                                        const TopologyPtr& topology,
                                                        bool accelerateJavaUDFs,
                                                        const std::string& accelerationServiceURL,
                                                        const Optimizer::SampleCodeGenerationPhasePtr& sampleCodeGenerationPhase);

  private:
    QueryId queryId;
    std::string queryString;
    QueryPlanPtr queryPlan;
    Optimizer::PlacementStrategy queryPlacementStrategy;
    z3::ContextPtr z3Context;
    QueryParsingServicePtr queryParsingService;
    FaultToleranceType faultToleranceType;

    const int32_t ELEGANT_SERVICE_TIMEOUT = 3000;

    //OpenCL payload constants
    const std::string DEVICE_INFO_KEY = "deviceInfo";
    const std::string DEVICE_INFO_NAME_KEY = "deviceName";
    const std::string DEVICE_INFO_DOUBLE_FP_SUPPORT_KEY = "doubleFPSupport";
    const std::string DEVICE_MAX_WORK_ITEMS_KEY = "maxWorkItems";
    const std::string DEVICE_MAX_WORK_ITEMS_DIM1_KEY = "dim1";
    const std::string DEVICE_MAX_WORK_ITEMS_DIM2_KEY = "dim2";
    const std::string DEVICE_MAX_WORK_ITEMS_DIM3_KEY = "dim3";
    const std::string DEVICE_INFO_ADDRESS_BITS_KEY = "deviceAddressBits";
    const std::string DEVICE_INFO_TYPE_KEY = "deviceType";
    const std::string DEVICE_INFO_EXTENSIONS_KEY = "deviceExtensions";
    const std::string DEVICE_INFO_AVAILABLE_PROCESSORS_KEY = "availableProcessors";
};
}// namespace RequestProcessor
}// namespace NES

#endif// NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_REQUESTTYPES_EXPLAINREQUEST_HPP_

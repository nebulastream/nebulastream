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

#ifndef NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_REQUESTTYPES_STOPQUERYREQUEST_HPP_
#define NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_REQUESTTYPES_STOPQUERYREQUEST_HPP_

#include <RequestProcessor/RequestTypes/AbstractRequest.hpp>
#include <RequestProcessor/RequestTypes/FailQueryRequest.hpp>
#include <RequestProcessor/StorageHandles/StorageHandler.hpp>

namespace NES::Optimizer {
class TypeInferencePhase;
using TypeInferencePhasePtr = std::shared_ptr<TypeInferencePhase>;

class QueryRewritePhase;
using QueryRewritePhasePtr = std::shared_ptr<QueryRewritePhase>;

class QueryPlacementPhase;
using QueryPlacementPhasePtr = std::shared_ptr<QueryPlacementPhase>;

class GlobalQueryPlanUpdatePhase;
using GlobalQueryPlanUpdatePhasePtr = std::shared_ptr<GlobalQueryPlanUpdatePhase>;
}// namespace NES::Optimizer

namespace NES {

class GlobalQueryPlan;
using GlobalQueryPlanPtr = std::shared_ptr<GlobalQueryPlan>;

class QueryCatalogService;
using QueryCatalogServicePtr = std::shared_ptr<QueryCatalogService>;

class QueryDeploymentPhase;
using QueryDeploymentPhasePtr = std::shared_ptr<QueryDeploymentPhase>;

class QueryUndeploymentPhase;
using QueryUndeploymentPhasePtr = std::shared_ptr<QueryUndeploymentPhase>;

class GlobalExecutionPlan;
using GlobalExecutionPlanPtr = std::shared_ptr<GlobalExecutionPlan>;

class Topology;
using TopologyPtr = std::shared_ptr<Topology>;

class WorkerRPCClient;
using WorkerRPCClientPtr = std::shared_ptr<WorkerRPCClient>;

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

namespace RequestProcessor::Experimental {

class StopQueryRequest;
using StopQueryRequestPtr = std::shared_ptr<StopQueryRequest>;

struct StopQueryResponse : public AbstractRequestResponse {
    explicit StopQueryResponse(bool success) : success(success){};
    bool success;
};

/**
 * @brief This request is used for stopping a running query in NES cluster
 */
class StopQueryRequest : public AbstractRequest {

  public:
    /**
     * @brief Construct a new Stop Query Request object
     * @param queryId The id of the query that we want to stop
     * @param maxRetries maximal number of retries to stop a query
     */
    StopQueryRequest(QueryId queryId, uint8_t maxRetries);

    /**
     * @brief creates a new Stop Query Request object
     * @param queryId The id of the query that we want to stop
     * @param maxRetries maximal number of retries to stop a query
     * @return a smart pointer to the newly created object
     */
    static StopQueryRequestPtr create(QueryId queryId, uint8_t maxRetries);

    std::string toString();

  protected:
    /**
     * @brief Executes the request logic.
     * @param storageHandle: a handle to access the coordinators data structures which might be needed for executing the
     * request
     * @throws QueryNotFoundException if the query or the associated shared query plan are not found
     * @throws InvalidQueryStatusException if the query's status is invalid for stop
     * @throws QueryPlacementException if the query placement phase fails
     * @throws RequestExecutionException if resource acquisition fails
     */
    std::vector<AbstractRequestPtr> executeRequestLogic(const StorageHandlerPtr& storageHandle) override;

    /**
     * @brief Roll back any changes made by a request that did not complete due to errors.
     * @param ex: The exception thrown during request execution.
     * @param storageHandle: The storage access handle that was used by the request to modify the system state.
     */
    std::vector<AbstractRequestPtr> rollBack(std::exception_ptr ex, const StorageHandlerPtr& storageHandle) override;

    /**
     * @brief Performs request specific error handling to be done before changes to the storage are rolled back
     * @param ex: The exception encountered
     * @param storageHandle: The storage access handle used by the request
     */
    void preRollbackHandle(std::exception_ptr ex, const StorageHandlerPtr& storageHandler) override;

    /**
     * @brief Performs request specific error handling to be done after changes to the storage are rolled back
     * @param ex: The exception encountered
     * @param storageHandle: The storage access handle used by the request
     */
    void postRollbackHandle(std::exception_ptr ex, const StorageHandlerPtr& storageHandler) override;

    /**
     * @brief Performs steps to be done after execution of the request logic, e.g. unlocking the required data structures
     * @param storageHandle: The storage access handle used by the request
     * @param requiredResources: The resources required during the execution phase
     */
    void postExecution(const StorageHandlerPtr& storageHandler) override;

  private:
    QueryId queryId;
    GlobalExecutionPlanPtr globalExecutionPlan;
    TopologyPtr topology;
    QueryCatalogServicePtr queryCatalogService;
    GlobalQueryPlanPtr globalQueryPlan;
    Catalogs::UDF::UDFCatalogPtr udfCatalog;
    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    QueryDeploymentPhasePtr queryDeploymentPhase;
    QueryUndeploymentPhasePtr queryUndeploymentPhase;
    Optimizer::TypeInferencePhasePtr typeInferencePhase;
    Optimizer::QueryPlacementPhasePtr queryPlacementPhase;
    Configurations::CoordinatorConfigurationPtr coordinatorConfiguration;
    static constexpr uint8_t MAX_RETRIES_FOR_FAILURE = 1;
};
}// namespace RequestProcessor::Experimental
}// namespace NES

#endif  // NES_COORDINATOR_INCLUDE_REQUESTPROCESSOR_REQUESTTYPES_STOPQUERYREQUEST_HPP_

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

#ifndef NES_CORE_INCLUDE_WORKQUEUES_REQUESTTYPES_EXPERIMENTAL_STOPQUERYREQUESTEXPERIMENTAL_HPP_
#define NES_CORE_INCLUDE_WORKQUEUES_REQUESTTYPES_EXPERIMENTAL_STOPQUERYREQUESTEXPERIMENTAL_HPP_

#include <WorkQueues/RequestTypes/AbstractRequest.hpp>
#include <WorkQueues/RequestTypes/Request.hpp>
#include <WorkQueues/StorageHandles/StorageHandler.hpp>

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

class StopQueryRequestExperimental;
using StopQueryRequestPtr = std::shared_ptr<StopQueryRequestExperimental>;

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

/**
 * @brief This request is used for stopping a running query in NES cluster
 */
class StopQueryRequestExperimental : public Request, public AbstractRequest {

  public:
    static StopQueryRequestPtr create(QueryId queryId,
                                      size_t maxRetries,
                                      WorkerRPCClientPtr workerRpcClient,
                                      Configurations::CoordinatorConfigurationPtr coordinatorConfiguration);

    void executeRequestLogic(StorageHandler& storageHandle) override;
    void preRollbackHandle(RequestExecutionException& ex, StorageHandler& storageHandle) override;
    void postRollbackHandle(RequestExecutionException& ex, StorageHandler& storageHandle) override;
    void rollBack(RequestExecutionException& ex, StorageHandler& storageHandle) override;
    void preExecution(StorageHandler& storageHandle) override;
    void postExecution(StorageHandler& storageHandle) override;
    std::string toString() override;

    ~StopQueryRequestExperimental() override = default;

  private:
    StopQueryRequestExperimental(QueryId queryId,
                                 size_t maxRetries,
                                 WorkerRPCClientPtr workerRpcClient,
                                 Configurations::CoordinatorConfigurationPtr coordinatorConfiguration);

    WorkerRPCClientPtr workerRpcClient;
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
};
}// namespace NES

#endif// NES_CORE_INCLUDE_WORKQUEUES_REQUESTTYPES_EXPERIMENTAL_STOPQUERYREQUESTEXPERIMENTAL_HPP_

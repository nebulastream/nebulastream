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

#ifndef NES_COORDINATOR_INCLUDE_SERVICES_REQUESTPROCESSORSERVICE_HPP_
#define NES_COORDINATOR_INCLUDE_SERVICES_REQUESTPROCESSORSERVICE_HPP_

#include <Optimizer/Phases/MemoryLayoutSelectionPhase.hpp>
#include <Optimizer/Phases/QueryMergerPhase.hpp>
#include <memory>

namespace z3 {
class context;
using ContextPtr = std::shared_ptr<context>;
}// namespace z3

namespace NES::Configurations {
class CoordinatorConfiguration;
using CoordinatorConfigurationPtr = std::shared_ptr<CoordinatorConfiguration>;
}// namespace NES::Configurations

namespace NES::Optimizer {
class TypeInferencePhase;
using TypeInferencePhasePtr = std::shared_ptr<TypeInferencePhase>;

class QueryRewritePhase;
using QueryRewritePhasePtr = std::shared_ptr<QueryRewritePhase>;

class QueryPlacementAmendmentPhase;
using QueryPlacementAmendmentPhasePtr = std::shared_ptr<QueryPlacementAmendmentPhase>;

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

class RequestQueue;
using RequestQueuePtr = std::shared_ptr<RequestQueue>;

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
 * @brief This service is started as a thread and is responsible for accessing the scheduling queue in the query catalog and executing the queryIdAndCatalogEntryMapping requests.
 */
class RequestProcessorService {
  public:
    explicit RequestProcessorService(const GlobalExecutionPlanPtr& globalExecutionPlan,
                                     const TopologyPtr& topology,
                                     const QueryCatalogServicePtr& queryCatalogService,
                                     const GlobalQueryPlanPtr& globalQueryPlan,
                                     const Catalogs::Source::SourceCatalogPtr& sourceCatalog,
                                     const Catalogs::UDF::UDFCatalogPtr& udfCatalog,
                                     const RequestQueuePtr& queryRequestQueue,
                                     const Configurations::CoordinatorConfigurationPtr& coordinatorConfiguration,
                                     const z3::ContextPtr& z3Context);

    /**
     * @brief Start the loop for processing new requests in the scheduling queue of the query catalog
     */
    void start();

    /**
     * @brief Indicate if query processor service is running
     * @return true if query processor is running
     */
    bool isQueryProcessorRunning();

    /**
     * @brief Stop query request processor service
     */
    void shutDown();

  private:
    std::mutex queryProcessorStatusLock;
    bool queryProcessorRunning;
    bool queryReconfiguration;
    QueryCatalogServicePtr queryCatalogService;
    Optimizer::TypeInferencePhasePtr typeInferencePhase;
    Optimizer::QueryPlacementAmendmentPhasePtr queryPlacementAmendmentPhase;
    QueryDeploymentPhasePtr queryDeploymentPhase;
    QueryUndeploymentPhasePtr queryUndeploymentPhase;
    RequestQueuePtr queryRequestQueue;
    GlobalQueryPlanPtr globalQueryPlan;
    GlobalExecutionPlanPtr globalExecutionPlan;
    Optimizer::GlobalQueryPlanUpdatePhasePtr globalQueryPlanUpdatePhase;
    Catalogs::UDF::UDFCatalogPtr udfCatalog;
    z3::ContextPtr z3Context;
};
}// namespace NES
#endif// NES_COORDINATOR_INCLUDE_SERVICES_REQUESTPROCESSORSERVICE_HPP_

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

#ifndef NES_INCLUDE_SERVICES_NES_REQUEST_PROCESSOR_SERVICE_HPP_
#define NES_INCLUDE_SERVICES_NES_REQUEST_PROCESSOR_SERVICE_HPP_

#include <Optimizer/Phases/MemoryLayoutSelectionPhase.hpp>
#include <Optimizer/Phases/QueryMergerPhase.hpp>
#include <memory>

namespace z3 {
class context;
using ContextPtr = std::shared_ptr<context>;
}// namespace z3

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

class QueryCatalog;
using QueryCatalogPtr = std::shared_ptr<QueryCatalog>;

class QueryDeploymentPhase;
using QueryDeploymentPhasePtr = std::shared_ptr<QueryDeploymentPhase>;

class QueryUndeploymentPhase;
using QueryUndeploymentPhasePtr = std::shared_ptr<QueryUndeploymentPhase>;

class SourceCatalog;
using SourceCatalogPtr = std::shared_ptr<SourceCatalog>;

class GlobalExecutionPlan;
using GlobalExecutionPlanPtr = std::shared_ptr<GlobalExecutionPlan>;

class Topology;
using TopologyPtr = std::shared_ptr<Topology>;

class WorkerRPCClient;
using WorkerRPCClientPtr = std::shared_ptr<WorkerRPCClient>;

class NESRequestQueue;
using NESRequestQueuePtr = std::shared_ptr<NESRequestQueue>;

/**
 * @brief This service is started as a thread and is responsible for accessing the scheduling queue in the query catalog and executing the queries requests.
 */
class NESRequestProcessorService {
  public:
    explicit NESRequestProcessorService(const GlobalExecutionPlanPtr& globalExecutionPlan,
                                        const TopologyPtr& topology,
                                        const QueryCatalogPtr& queryCatalog,
                                        const GlobalQueryPlanPtr& globalQueryPlan,
                                        const SourceCatalogPtr& streamCatalog,
                                        const WorkerRPCClientPtr& workerRpcClient,
                                        NESRequestQueuePtr queryRequestQueue,
                                        Optimizer::QueryMergerRule queryMergerRule,
                                        Optimizer::MemoryLayoutSelectionPhase::MemoryLayoutPolicy memoryLayoutPolicy,
                                        bool performOnlySourceOperatorExpansion);

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
    QueryCatalogPtr queryCatalog;
    Optimizer::TypeInferencePhasePtr typeInferencePhase;
    Optimizer::QueryPlacementPhasePtr queryPlacementPhase;
    QueryDeploymentPhasePtr queryDeploymentPhase;
    QueryUndeploymentPhasePtr queryUndeploymentPhase;
    NESRequestQueuePtr queryRequestQueue;
    GlobalQueryPlanPtr globalQueryPlan;
    Optimizer::GlobalQueryPlanUpdatePhasePtr globalQueryPlanUpdatePhase;
    z3::ContextPtr z3Context;
};
}// namespace NES
#endif// NES_INCLUDE_SERVICES_NES_REQUEST_PROCESSOR_SERVICE_HPP_

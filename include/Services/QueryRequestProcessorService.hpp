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

#ifndef NES_QUERYREQUESTPROCESSORSERVICE_HPP
#define NES_QUERYREQUESTPROCESSORSERVICE_HPP

#include <memory>

namespace z3 {
class context;
typedef std::shared_ptr<context> ContextPtr;
}// namespace z3

namespace NES {

class GlobalQueryPlan;
typedef std::shared_ptr<GlobalQueryPlan> GlobalQueryPlanPtr;

class QueryCatalog;
typedef std::shared_ptr<QueryCatalog> QueryCatalogPtr;

class TypeInferencePhase;
typedef std::shared_ptr<TypeInferencePhase> TypeInferencePhasePtr;

class QueryRewritePhase;
typedef std::shared_ptr<QueryRewritePhase> QueryRewritePhasePtr;

class QueryPlacementPhase;
typedef std::shared_ptr<QueryPlacementPhase> QueryPlacementPhasePtr;

class QueryDeploymentPhase;
typedef std::shared_ptr<QueryDeploymentPhase> QueryDeploymentPhasePtr;

class QueryUndeploymentPhase;
typedef std::shared_ptr<QueryUndeploymentPhase> QueryUndeploymentPhasePtr;

class GlobalQueryPlanUpdatePhase;
typedef std::shared_ptr<GlobalQueryPlanUpdatePhase> GlobalQueryPlanUpdatePhasePtr;

class StreamCatalog;
typedef std::shared_ptr<StreamCatalog> StreamCatalogPtr;

class GlobalExecutionPlan;
typedef std::shared_ptr<GlobalExecutionPlan> GlobalExecutionPlanPtr;

class Topology;
typedef std::shared_ptr<Topology> TopologyPtr;

class WorkerRPCClient;
typedef std::shared_ptr<WorkerRPCClient> WorkerRPCClientPtr;

class QueryRequestQueue;
typedef std::shared_ptr<QueryRequestQueue> QueryRequestQueuePtr;

/**
 * @brief This service is started as a thread and is responsible for accessing the scheduling queue in the query catalog and executing the queries requests.
 */
class QueryRequestProcessorService {
  public:
    explicit QueryRequestProcessorService(GlobalExecutionPlanPtr globalExecutionPlan, TopologyPtr topology,
                                          QueryCatalogPtr queryCatalog, GlobalQueryPlanPtr globalQueryPlan,
                                          StreamCatalogPtr streamCatalog, WorkerRPCClientPtr workerRpcClient,
                                          QueryRequestQueuePtr queryRequestQueue, bool enableQueryMerging,
                                          std::string queryMergerRule);

    ~QueryRequestProcessorService();
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
    TypeInferencePhasePtr typeInferencePhase;
    QueryRewritePhasePtr queryRewritePhase;
    QueryPlacementPhasePtr queryPlacementPhase;
    QueryDeploymentPhasePtr queryDeploymentPhase;
    QueryUndeploymentPhasePtr queryUndeploymentPhase;
    QueryRequestQueuePtr queryRequestQueue;
    GlobalQueryPlanPtr globalQueryPlan;
    GlobalQueryPlanUpdatePhasePtr globalQueryPlanUpdatePhase;
    z3::ContextPtr z3Context;
};
}// namespace NES
#endif//NES_QUERYREQUESTPROCESSORSERVICE_HPP

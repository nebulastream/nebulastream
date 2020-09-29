#ifndef NES_QUERYREQUESTPROCESSORSERVICE_HPP
#define NES_QUERYREQUESTPROCESSORSERVICE_HPP

#include <memory>

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

class QueryPlacementRefinementPhase;
typedef std::shared_ptr<QueryPlacementRefinementPhase> QueryPlacementRefinementPhasePtr;

class QueryDeploymentPhase;
typedef std::shared_ptr<QueryDeploymentPhase> QueryDeploymentPhasePtr;

class QueryUndeploymentPhase;
typedef std::shared_ptr<QueryUndeploymentPhase> QueryUndeploymentPhasePtr;

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
    explicit QueryRequestProcessorService(GlobalExecutionPlanPtr globalExecutionPlan, TopologyPtr topology, QueryCatalogPtr queryCatalog,
                                          StreamCatalogPtr streamCatalog, WorkerRPCClientPtr workerRpcClient, QueryRequestQueuePtr queryRequestQueue);

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
    QueryPlacementRefinementPhasePtr queryPlacementRefinementPhase;
    QueryDeploymentPhasePtr queryDeploymentPhase;
    QueryUndeploymentPhasePtr queryUndeploymentPhase;
    QueryRequestQueuePtr queryRequestQueue;
    GlobalQueryPlanPtr globalQueryPlan;

};
}// namespace NES
#endif//NES_QUERYREQUESTPROCESSORSERVICE_HPP

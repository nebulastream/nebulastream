#ifndef NES_QUERYREQUESTPROCESSORSERVICE_HPP
#define NES_QUERYREQUESTPROCESSORSERVICE_HPP

#include <memory>

namespace NES {

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

class StreamCatalog;
typedef std::shared_ptr<StreamCatalog> StreamCatalogPtr;

class GlobalExecutionPlan;
typedef std::shared_ptr<GlobalExecutionPlan> GlobalExecutionPlanPtr;

class NESTopologyPlan;
typedef std::shared_ptr<NESTopologyPlan> NESTopologyPlanPtr;

class WorkerRPCClient;
typedef std::shared_ptr<WorkerRPCClient> WorkerRPCClientPtr;

class QueryDeployer;
typedef std::shared_ptr<QueryDeployer> QueryDeployerPtr;

/**
 * @brief This service is started as a thread and is responsible for accessing the scheduling queue in the query catalog and executing the queries requests.
 */
class QueryRequestProcessorService {
  public:
    explicit QueryRequestProcessorService(GlobalExecutionPlanPtr globalExecutionPlan, NESTopologyPlanPtr nesTopologyPlan, QueryCatalogPtr queryCatalog,
                                          StreamCatalogPtr streamCatalog, WorkerRPCClientPtr workerRpcClient, QueryDeployerPtr queryDeployer);

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
    QueryUndeploymentPhasePtr queryUndeploymentPhase;
    QueryDeploymentPhasePtr queryDeploymentPhase;
};
}// namespace NES
#endif//NES_QUERYREQUESTPROCESSORSERVICE_HPP

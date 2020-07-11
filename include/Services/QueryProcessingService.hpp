#ifndef NES_QUERYPROCESSINGSERVICE_HPP
#define NES_QUERYPROCESSINGSERVICE_HPP

#include <memory>

namespace NES {

class QueryCatalog;
typedef std::shared_ptr<QueryCatalog> QueryCatalogPtr;

class TypeInferencePhase;
typedef std::shared_ptr<TypeInferencePhase> TypeInferencePhasePtr;

class StreamCatalog;
typedef std::shared_ptr<StreamCatalog> StreamCatalogPtr;

class QueryDeployer;
typedef std::shared_ptr<QueryDeployer> QueryDeployerPtr;

class WorkerRPCClient;
typedef std::shared_ptr<WorkerRPCClient> WorkerRPCClientPtr;

class QueryService;
typedef std::shared_ptr<QueryService> QueryServicePtr;

class QueryProcessingService {
  public:
    explicit QueryProcessingService(GlobalExecutionPlanPtr globalExecutionPlan, NESTopologyPlanPtr nesTopologyPlan, QueryCatalogPtr queryCatalog,
                                      StreamCatalogPtr streamCatalog, QueryDeployerPtr queryDeployer, WorkerRPCClientPtr workerRPCClient);
    int operator()();

    /**
     * @brief Indicate if query processor service is running
     * @return true if query processor is running
     */
    bool isQueryProcessorRunning() const;

  private:

    bool queryProcessorRunning = true;
    GlobalExecutionPlanPtr globalExecutionPlan;
    QueryCatalogPtr queryCatalog;
    TypeInferencePhasePtr typeInferencePhase;
    QueryRewritePhasePtr queryRewritePhase;
    QueryPlacementPhasePtr queryPlacementPhase;
    QueryDeployerPtr queryDeployer;
    WorkerRPCClientPtr workerRPCClient;
    QueryServicePtr queryService;
};
}// namespace NES
#endif//NES_QUERYPROCESSINGSERVICE_HPP

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

class QueryProcessingService {
  public:
    explicit QueryProcessingService(GlobalExecutionPlanPtr globalExecutionPlan, NESTopologyPlanPtr nesTopologyPlan, QueryCatalogPtr queryCatalog,
                                      StreamCatalogPtr streamCatalog, QueryDeployerPtr queryDeployer, WorkerRPCClientPtr workerRPCClient);
    int operator()();

  private:

    /**
     * @brief Deploys the processed query to the respective worker nodes
     * @param queryId : Id of the query to be deployed
     * @return true if successful
     */
    bool deployQuery(std::string queryId);
    bool stopQueryProcessor = false;
    GlobalExecutionPlanPtr globalExecutionPlan;
    QueryCatalogPtr queryCatalog;
    TypeInferencePhasePtr typeInferencePhase;
    QueryRewritePhasePtr queryRewritePhase;
    QueryPlacementPhasePtr queryPlacementPhase;
    QueryDeployerPtr queryDeployer;
    WorkerRPCClientPtr workerRPCClient;
};
}// namespace NES
#endif//NES_QUERYPROCESSINGSERVICE_HPP

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
    void stopQueryRequestProcessor();

  private:
    /**
     * @brief method for deploying and starting the query
     * @param queryId : the query Id of the query to be deployed and started
     * @return true if successful else false
     */
    bool deployAndStartQuery(std::string queryId);

    /**
     * @brief method for stopping and undeploying the query with the given id
     * @param queryId : id of the query
     * @return true if successful
     */
    bool stopAndUndeployQuery(const std::string queryId);

    /**
     * @brief method send query to nodes
     * @param queryId
     * @return bool indicating success
     */
    bool deployQuery(std::string queryId, std::vector<ExecutionNodePtr> executionNodes);

    /**
     * @brief method to start a already deployed query
     * @param queryId
     * @return bool indicating success
     */
    bool startQuery(std::string queryId, std::vector<ExecutionNodePtr> executionNodes);

    /**
     * @brief method remove query from nodes
     * @param queryId
     * @return bool indicating success
     */
    bool undeployQuery(std::string queryId, std::vector<ExecutionNodePtr> executionNodes);

    /**
     * @brief method to stop a query
     * @param queryId
     * @return bool indicating success
     */
    bool stopQuery(std::string queryId, std::vector<ExecutionNodePtr> executionNodes);

    std::mutex queryProcessorLock;
    std::mutex queryProcessorStatusLock;
    bool queryProcessorRunning;
    GlobalExecutionPlanPtr globalExecutionPlan;
    QueryCatalogPtr queryCatalog;
    TypeInferencePhasePtr typeInferencePhase;
    QueryRewritePhasePtr queryRewritePhase;
    QueryPlacementPhasePtr queryPlacementPhase;
    WorkerRPCClientPtr workerRPCClient;
    QueryDeployerPtr queryDeployer;
};
}// namespace NES
#endif//NES_QUERYREQUESTPROCESSORSERVICE_HPP

#ifndef INCLUDE_COMPONENTS_NESCOORDINATOR_HPP_
#define INCLUDE_COMPONENTS_NESCOORDINATOR_HPP_

#include <GRPC/WorkerRPCClient.hpp>
#include <future>
#include <grpcpp/grpcpp.h>
#include <grpcpp/server_builder.h>
#include <string>
#include <thread>
namespace NES {
class QueryRequestQueue;
typedef std::shared_ptr<QueryRequestQueue> QueryRequestQueuePtr;

class Topology;
typedef std::shared_ptr<Topology> TopologyPtr;

class GlobalExecutionPlan;
typedef std::shared_ptr<GlobalExecutionPlan> GlobalExecutionPlanPtr;

class QueryCatalog;
typedef std::shared_ptr<QueryCatalog> QueryCatalogPtr;

class StreamCatalog;
typedef std::shared_ptr<StreamCatalog> StreamCatalogPtr;

class RestServer;
typedef std::shared_ptr<RestServer> RestServerPtr;

class CoordinatorEngine;
typedef std::shared_ptr<CoordinatorEngine> CoordinatorEnginePtr;

class QueryDeployer;
typedef std::shared_ptr<QueryDeployer> QueryDeployerPtr;

class NesWorker;
typedef std::shared_ptr<NesWorker> NesWorkerPtr;

class QueryStatistics;
typedef std::shared_ptr<QueryStatistics> QueryStatisticsPtr;

class QueryRequestProcessorService;
typedef std::shared_ptr<QueryRequestProcessorService> QueryRequestProcessorServicePtr;

class QueryService;
typedef std::shared_ptr<QueryService> QueryServicePtr;

class MonitoringPlan;
typedef std::shared_ptr<MonitoringPlan> MonitoringPlanPtr;

class NesCoordinator : public std::enable_shared_from_this<NesCoordinator> {
  public:
    explicit NesCoordinator(std::string serverIp, uint16_t restPort, uint16_t rpcPort, uint16_t numberOfCpus);

    /**
     * @brief constructor with default numberOfCpus
     */
    NesCoordinator(std::string serverIp, uint16_t restPort, uint16_t rpcPort);
    /**
     * @brief dtor
     * @return
     */
    ~NesCoordinator();

    /**
     * @brief start rpc server: rest server, and one worker <
     * @param bool if the method should block
     */
    size_t startCoordinator(bool blocking);

    /**
     * @brief method to stop coordinator
     * @param force the shutdown even when queries are running
     * @return bool indicating success
     */
    bool stopCoordinator(bool force);

    /**
    * @brief method to return the query statistics
    * @param id of the query
    * @return vector of queryStatistics
    */
    std::vector<QueryStatisticsPtr> getQueryStatistics(QueryId queryId);

    /**
     * @brief method to overwrite server ip
     * @param serverIp
     */
    void setServerIp(std::string serverIp);

    /**
     * @brief catalog method for debug use only
     * @return streamCatalog
     */
    StreamCatalogPtr getStreamCatalog() {
        return streamCatalog;
    }

    TopologyPtr getTopology() {
        return topology;
    }

    /**
     * @brief Get the instance of query service
     * @return Query service pointer
     */
    QueryServicePtr getQueryService();

    /**
     * @brief Get instance of query catalog
     * @return query catalog pointer
     */
    QueryCatalogPtr getQueryCatalog();

    /**
     * @brief Requests from a remote worker node its monitoring data.
     * @param ipAddress
     * @param grpcPort
     * @param the mutable schema to be extended
     * @param the buffer where the data will be written into
     * @return true if successful, else false
     */
    SchemaPtr requestMonitoringData(const std::string& ipAddress, int64_t grpcPort, MonitoringPlanPtr plan, TupleBuffer buf);

  private:
    /**
     * @brief this method will start the GRPC Coordinator server which is responsible for reacting to calls from the CoordinatorRPCClient
     */
    void buildAndStartGRPCServer(std::promise<bool>& prom);

  private:
    std::string serverIp;
    uint16_t restPort;
    uint16_t rpcPort;
    uint16_t numberOfCpus;
    std::unique_ptr<grpc::Server> rpcServer;
    std::shared_ptr<std::thread> rpcThread;
    std::shared_ptr<std::thread> queryRequestProcessorThread;
    NesWorkerPtr worker;
    CoordinatorEnginePtr coordinatorEngine;
    GlobalExecutionPlanPtr globalExecutionPlan;
    QueryCatalogPtr queryCatalog;
    StreamCatalogPtr streamCatalog;
    TopologyPtr topology;
    RestServerPtr restServer;
    std::shared_ptr<std::thread> restThread;
    std::atomic<bool> stopped;
    QueryRequestProcessorServicePtr queryRequestProcessorService;
    QueryServicePtr queryService;
    WorkerRPCClientPtr workerRpcClient;
    QueryRequestQueuePtr queryRequestQueue;
};
typedef std::shared_ptr<NesCoordinator> NesCoordinatorPtr;

}// namespace NES
#endif /* INCLUDE_COMPONENTS_NESCOORDINATOR_HPP_ */

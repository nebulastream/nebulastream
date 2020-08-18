#ifndef INCLUDE_COMPONENTS_NESCOORDINATOR_HPP_
#define INCLUDE_COMPONENTS_NESCOORDINATOR_HPP_

#include <GRPC/WorkerRPCClient.hpp>
#include <grpcpp/grpcpp.h>
#include <grpcpp/server_builder.h>
#include <string>
#include <thread>

namespace NES {

class TopologyManager;
typedef std::shared_ptr<TopologyManager> TopologyManagerPtr;

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

class NesCoordinator : public std::enable_shared_from_this<NesCoordinator> {
  public:
    explicit NesCoordinator(std::string serverIp, uint16_t restPort, uint16_t rpcPort);

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
    * @return queryStatistics
    */
    QueryStatisticsPtr getQueryStatistics(QueryId queryId);

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

    TopologyManagerPtr getTopologyManger() {
        return topologyManager;
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

  private:
    /**
     * @brief this method will start the GRPC Coordinator server which is responsible for reacting to calls from the
     * CoordinatorRPCClient
     */
    void buildAndStartGRPCServer();

  private:
    std::string serverIp;
    uint16_t restPort;
    uint16_t rpcPort;
    std::shared_ptr<grpc::Server> rpcServer;
    std::shared_ptr<std::thread> rpcThread;
    std::shared_ptr<std::thread> queryRequestProcessorThread;
    NesWorkerPtr worker;
    CoordinatorEnginePtr coordinatorEngine;
    GlobalExecutionPlanPtr globalExecutionPlan;
    QueryCatalogPtr queryCatalog;
    StreamCatalogPtr streamCatalog;
    TopologyManagerPtr topologyManager;
    RestServerPtr restServer;
    std::shared_ptr<std::thread> restThread;
    std::atomic<bool> stopped;
    QueryRequestProcessorServicePtr queryRequestProcessorService;
    QueryServicePtr queryService;
};
typedef std::shared_ptr<NesCoordinator> NesCoordinatorPtr;

}// namespace NES
#endif /* INCLUDE_COMPONENTS_NESCOORDINATOR_HPP_ */

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

#ifndef INCLUDE_COMPONENTS_NESCOORDINATOR_HPP_
#define INCLUDE_COMPONENTS_NESCOORDINATOR_HPP_

#include <GRPC/WorkerRPCClient.hpp>
#include <NodeEngine/NodeEngineForwaredRefs.hpp>
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

class QueryRequestProcessorService;
typedef std::shared_ptr<QueryRequestProcessorService> QueryRequestProcessorServicePtr;

class QueryService;
typedef std::shared_ptr<QueryService> QueryServicePtr;

class MonitoringService;
typedef std::shared_ptr<MonitoringService> MonitoringServicePtr;

class GlobalQueryPlan;
typedef std::shared_ptr<GlobalQueryPlan> GlobalQueryPlanPtr;

class NesCoordinator : public std::enable_shared_from_this<NesCoordinator> {
  public:
    explicit NesCoordinator(std::string restIp, uint16_t restPort, std::string rpcIp, uint16_t rpcPort,
                            uint16_t numberOfSlots = std::thread::hardware_concurrency(), bool enableQueryMerging = false);

    /**
     * @brief Constructor where ip = restIp and rpcIp
     */
    NesCoordinator(const std::string& ip, uint16_t restPort, uint16_t rpcPort,
                   uint16_t numberOfSlots = std::thread::hardware_concurrency(), bool enableQueryMerging = false);

    /**
     * @brief dtor
     * @return
     */
    ~NesCoordinator();

    /**
     * @brief start rpc server: rest server, and one worker <
     * @param bool if the method should block
     */
    uint64_t startCoordinator(bool blocking);

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
    std::vector<NodeEngine::QueryStatisticsPtr> getQueryStatistics(QueryId queryId);

    /**
     * @brief catalog method for debug use only
     * @return streamCatalog
     */
    StreamCatalogPtr getStreamCatalog() { return streamCatalog; }

    TopologyPtr getTopology() { return topology; }

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
     * @brief Get instance of monitoring service
     * @return monitoring service pointer
     */
    MonitoringServicePtr getMonitoringService();

    /**
     * @brief Get the instance of Global Query Plan
     * @return Global query plan
     */
    GlobalQueryPlanPtr getGlobalQueryPlan();

  private:
    /**
     * @brief this method will start the GRPC Coordinator server which is responsible for reacting to calls from the CoordinatorRPCClient
     */
    void buildAndStartGRPCServer(std::promise<bool>& prom);

  private:
    std::string restIp;
    uint16_t restPort;
    std::string rpcIp;
    uint16_t rpcPort;
    uint16_t numberOfSlots;
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
    MonitoringServicePtr monitoringService;
    WorkerRPCClientPtr workerRpcClient;
    QueryRequestQueuePtr queryRequestQueue;
    GlobalQueryPlanPtr globalQueryPlan;
};
typedef std::shared_ptr<NesCoordinator> NesCoordinatorPtr;

}// namespace NES
#endif /* INCLUDE_COMPONENTS_NESCOORDINATOR_HPP_ */

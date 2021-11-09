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

#include <Configurations/ConfigOptions/CoordinatorConfig.hpp>
#include <Configurations/Persistence/StreamCatalogPersistence.hpp>
#include <Configurations/Persistence/StreamCatalogPersistenceFactory.hpp>
#include <NodeEngine/ErrorListener.hpp>
#include <NodeEngine/NodeEngineForwaredRefs.hpp>
#include <Plans/Query/QueryId.hpp>
#include <Util/VirtualEnableSharedFromThis.hpp>
#include <future>
#include <string>
#include <thread>
#include <vector>

namespace grpc {
class Server;
}
namespace NES {
class NESRequestQueue;
typedef std::shared_ptr<NESRequestQueue> NESRequestQueuePtr;

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

class NESRequestProcessorService;
typedef std::shared_ptr<NESRequestProcessorService> QueryRequestProcessorServicePtr;

class QueryService;
typedef std::shared_ptr<QueryService> QueryServicePtr;

class MonitoringService;
typedef std::shared_ptr<MonitoringService> MonitoringServicePtr;

class GlobalQueryPlan;
typedef std::shared_ptr<GlobalQueryPlan> GlobalQueryPlanPtr;

class WorkerRPCClient;
typedef std::shared_ptr<WorkerRPCClient> WorkerRPCClientPtr;

class NesCoordinator : public detail::virtual_enable_shared_from_this<NesCoordinator>, public ErrorListener {
    // virtual_enable_shared_from_this necessary for double inheritance of enable_shared_from_this
    typedef detail::virtual_enable_shared_from_this<NesCoordinator> inherited0;
    typedef ErrorListener inherited1;

  public:
    explicit NesCoordinator(CoordinatorConfigPtr coordinatorConfig);

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

    NodeEngine::NodeEnginePtr getNodeEngine();

    void onFatalError(int signalNumber, std::string string) override;
    void onFatalException(const std::shared_ptr<std::exception> ptr, std::string string) override;

    /**
     * @brief Method to check if a coordinator is still running
     * @return running status of the coordinator
     */
    bool isCoordinatorRunning();

    /**
     * getter for the coordinator engine
     * @return
     */
    const CoordinatorEnginePtr getCoordinatorEngine() const;

    NesWorkerPtr getNesWorker();

  private:
    /**
     * @brief this method will start the GRPC Coordinator server which is responsible for reacting to calls from the CoordinatorRPCClient
     */
    void buildAndStartGRPCServer(std::shared_ptr<std::promise<bool>> prom);

  private:
    std::string restIp;
    uint16_t restPort;
    std::string rpcIp;
    uint16_t rpcPort;
    uint16_t numberOfSlots;
    uint16_t numberOfWorkerThreads;
    uint32_t numberOfBuffersInGlobalBufferManager;
    uint32_t numberOfBuffersPerPipeline;
    uint32_t numberOfBuffersInSourceLocalBufferPool;
    uint64_t bufferSizeInBytes;
    StreamCatalogPersistenceType persistenceType;
    std::string persistenceDir;

    std::unique_ptr<grpc::Server> rpcServer;
    std::shared_ptr<std::thread> rpcThread;
    std::shared_ptr<std::thread> queryRequestProcessorThread;
    NesWorkerPtr worker;
    CoordinatorEnginePtr coordinatorEngine;

  private:
    GlobalExecutionPlanPtr globalExecutionPlan;
    QueryCatalogPtr queryCatalog;
    StreamCatalogPtr streamCatalog;
    StreamCatalogPersistencePtr persistence;
    TopologyPtr topology;
    RestServerPtr restServer;
    std::shared_ptr<std::thread> restThread;
    std::atomic<bool> isRunning;
    QueryRequestProcessorServicePtr queryRequestProcessorService;
    QueryServicePtr queryService;
    MonitoringServicePtr monitoringService;
    WorkerRPCClientPtr workerRpcClient;
    NESRequestQueuePtr queryRequestQueue;
    GlobalQueryPlanPtr globalQueryPlan;
};
typedef std::shared_ptr<NesCoordinator> NesCoordinatorPtr;

}// namespace NES
#endif /* INCLUDE_COMPONENTS_NESCOORDINATOR_HPP_ */

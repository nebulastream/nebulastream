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

#ifndef NES_INCLUDE_COMPONENTS_NESCOORDINATOR_HPP_
#define NES_INCLUDE_COMPONENTS_NESCOORDINATOR_HPP_

#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Plans/Query/QueryId.hpp>
#include <Runtime/ErrorListener.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Services/StreamCatalogService.hpp>
#include <Services/TopologyManagerService.hpp>
#include <Util/VirtualEnableSharedFromThis.hpp>
#include <future>
#include <string>
#include <thread>
#include <vector>

namespace grpc {
class Server;
}
namespace NES {

using namespace Configurations;

class RequestQueue;
using RequestQueuePtr = std::shared_ptr<RequestQueue>;

class Topology;
using TopologyPtr = std::shared_ptr<Topology>;

class GlobalExecutionPlan;
using GlobalExecutionPlanPtr = std::shared_ptr<GlobalExecutionPlan>;

class QueryCatalog;
using QueryCatalogPtr = std::shared_ptr<QueryCatalog>;

class SourceCatalog;
using SourceCatalogPtr = std::shared_ptr<SourceCatalog>;

class RestServer;
using RestServerPtr = std::shared_ptr<RestServer>;

class QueryDeployer;
using QueryDeployerPtr = std::shared_ptr<QueryDeployer>;

class NesWorker;
using NesWorkerPtr = std::shared_ptr<NesWorker>;

class RequestProcessorService;
using QueryRequestProcessorServicePtr = std::shared_ptr<RequestProcessorService>;

class QueryService;
using QueryServicePtr = std::shared_ptr<QueryService>;

class MonitoringService;
using MonitoringServicePtr = std::shared_ptr<MonitoringService>;

class GlobalQueryPlan;
using GlobalQueryPlanPtr = std::shared_ptr<GlobalQueryPlan>;

class WorkerRPCClient;
using WorkerRPCClientPtr = std::shared_ptr<WorkerRPCClient>;

class StreamCatalogService;
using StreamCatalogServicePtr = std::shared_ptr<StreamCatalogService>;

class TopologyManagerService;
using TopologyManagerServicePtr = std::shared_ptr<TopologyManagerService>;

namespace Catalogs {

class UdfCatalog;
using UdfCatalogPtr = std::shared_ptr<UdfCatalog>;

}// namespace Catalogs

class NesCoordinator : public detail::virtual_enable_shared_from_this<NesCoordinator>, public ErrorListener {
    // virtual_enable_shared_from_this necessary for double inheritance of enable_shared_from_this
    using inherited0 = detail::virtual_enable_shared_from_this<NesCoordinator>;
    using inherited1 = ErrorListener;

  public:
    explicit NesCoordinator(CoordinatorConfigurationPtr coordinatorConfig);

    explicit NesCoordinator(CoordinatorConfigurationPtr coordinatorConfig, WorkerConfigurationPtr workerConfigPtr);

    /**
     * @brief dtor
     * @return
     */
    ~NesCoordinator() override;

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
    std::vector<Runtime::QueryStatisticsPtr> getQueryStatistics(QueryId queryId);

    /**
     * @brief catalog method for debug use only
     * @return sourceCatalog
     */
    SourceCatalogPtr getStreamCatalog() const { return streamCatalog; }

    TopologyPtr getTopology() const { return topology; }

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
     * @brief Return the UDF catalog.
     * @return Pointer to the UDF catalog.
     */
    Catalogs::UdfCatalogPtr getUdfCatalog();

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

    Runtime::NodeEnginePtr getNodeEngine();

    void onFatalError(int signalNumber, std::string string) override;
    void onFatalException(std::shared_ptr<std::exception> ptr, std::string string) override;

    /**
     * @brief Method to check if a coordinator is still running
     * @return running status of the coordinator
     */
    bool isCoordinatorRunning();

    /**
     * getter for the streamCatalogService
     * @return
     */
    StreamCatalogServicePtr getStreamCatalogService() const;

    /**
     * getter for the topologyManagerService
     * @return
     */
    TopologyManagerServicePtr getTopologyManagerService() const;

    NesWorkerPtr getNesWorker();

  private:
    /**
     * @brief this method will start the GRPC Coordinator server which is responsible for reacting to calls from the CoordinatorRPCClient
     */
    void buildAndStartGRPCServer(const std::shared_ptr<std::promise<bool>>& prom);

    std::string restIp;
    uint16_t restPort;
    std::string rpcIp;
    uint16_t rpcPort;
    uint16_t numberOfSlots;
    uint16_t numberOfWorkerThreads;
    uint32_t numberOfBuffersInGlobalBufferManager;
    uint32_t numberOfBuffersPerWorker;
    uint32_t numberOfBuffersInSourceLocalBufferPool;
    uint64_t bufferSizeInBytes;
    std::unique_ptr<grpc::Server> rpcServer;
    std::shared_ptr<std::thread> rpcThread;
    std::shared_ptr<std::thread> queryRequestProcessorThread;
    NesWorkerPtr worker;
    TopologyManagerServicePtr topologyManagerService;
    StreamCatalogServicePtr streamCatalogService;
    GlobalExecutionPlanPtr globalExecutionPlan;
    QueryCatalogPtr queryCatalog;
    SourceCatalogPtr streamCatalog;
    TopologyPtr topology;
    RestServerPtr restServer;
    std::shared_ptr<std::thread> restThread;
    std::atomic<bool> isRunning{false};
    QueryRequestProcessorServicePtr queryRequestProcessorService;
    QueryServicePtr queryService;
    MonitoringServicePtr monitoringService;
    WorkerRPCClientPtr workerRpcClient;
    RequestQueuePtr queryRequestQueue;
    GlobalQueryPlanPtr globalQueryPlan;
    WorkerConfigurationPtr workerConfig;
    Catalogs::UdfCatalogPtr udfCatalog;
    bool enableMonitoring;
};
using NesCoordinatorPtr = std::shared_ptr<NesCoordinator>;

}// namespace NES
#endif  // NES_INCLUDE_COMPONENTS_NESCOORDINATOR_HPP_

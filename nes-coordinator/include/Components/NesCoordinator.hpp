/*
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

#ifndef NES_COORDINATOR_INCLUDE_COMPONENTS_NESCOORDINATOR_HPP_
#define NES_COORDINATOR_INCLUDE_COMPONENTS_NESCOORDINATOR_HPP_

#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Exceptions/ErrorListener.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Util/VirtualEnableSharedFromThis.hpp>
#include <folly/concurrency/UnboundedQueue.h>
#include <future>
#include <string>
#include <thread>
#include <vector>

namespace grpc {
class Server;
}
namespace NES {

namespace Statistic {
class StatisticProbeHandler;
using StatisticProbeHandlerPtr = std::shared_ptr<StatisticProbeHandler>;
}// namespace Statistic

namespace Runtime {

class QueryStatistics;
using QueryStatisticsPtr = std::shared_ptr<QueryStatistics>;

class NodeEngine;
using NodeEnginePtr = std::shared_ptr<NodeEngine>;

}// namespace Runtime
using namespace Configurations;

class RequestQueue;
using RequestQueuePtr = std::shared_ptr<RequestQueue>;

class Topology;
using TopologyPtr = std::shared_ptr<Topology>;

class RestServer;
using RestServerPtr = std::shared_ptr<RestServer>;

class NesWorker;
using NesWorkerPtr = std::shared_ptr<NesWorker>;

class RequestHandlerService;
using RequestHandlerServicePtr = std::shared_ptr<RequestHandlerService>;

class QueryParsingService;
using QueryParsingServicePtr = std::shared_ptr<QueryParsingService>;

class MonitoringService;
using MonitoringServicePtr = std::shared_ptr<MonitoringService>;

class GlobalQueryPlan;
using GlobalQueryPlanPtr = std::shared_ptr<GlobalQueryPlan>;

class WorkerRPCClient;
using WorkerRPCClientPtr = std::shared_ptr<WorkerRPCClient>;

class CoordinatorHealthCheckService;
using CoordinatorHealthCheckServicePtr = std::shared_ptr<CoordinatorHealthCheckService>;

class LocationService;
using LocationServicePtr = std::shared_ptr<LocationService>;

namespace Catalogs {

namespace Source {
class SourceCatalog;
using SourceCatalogPtr = std::shared_ptr<SourceCatalog>;
}// namespace Source

namespace Query {
class QueryCatalog;
using QueryCatalogPtr = std::shared_ptr<QueryCatalog>;
}// namespace Query

namespace UDF {
class UDFCatalog;
using UDFCatalogPtr = std::shared_ptr<UDFCatalog>;
}// namespace UDF
}// namespace Catalogs

namespace Optimizer {
class GlobalExecutionPlan;
using GlobalExecutionPlanPtr = std::shared_ptr<GlobalExecutionPlan>;

class PlacementAmendmentHandler;
using PlacementAmendmentHandlerPtr = std::shared_ptr<PlacementAmendmentHandler>;
}// namespace Optimizer

class NesCoordinator : public detail::virtual_enable_shared_from_this<NesCoordinator>, public Exceptions::ErrorListener {
    // virtual_enable_shared_from_this necessary for double inheritance of enable_shared_from_this
    using inherited0 = detail::virtual_enable_shared_from_this<NesCoordinator>;
    using inherited1 = ErrorListener;

  public:
    explicit NesCoordinator(CoordinatorConfigurationPtr coordinatorConfig);

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
     * @param force the shutdown even when queryIdAndCatalogEntryMapping are running
     * @return bool indicating success
     */
    bool stopCoordinator(bool force);

    /**
    * @brief method to return the query statistics
    * @param id of the shared query
    * @return vector of queryStatistics
    */
    std::vector<Runtime::QueryStatisticsPtr> getQueryStatistics(SharedQueryId sharedQueryId);

    /**
     * @brief catalog method for debug use only
     * @return sourceCatalog
     */
    Catalogs::Source::SourceCatalogPtr getSourceCatalog() const;

    /**
     * @brief get topology of coordinator
     * @return topology
     */
    TopologyPtr getTopology() const;

    /**
     * @brief Get the instance of query service
     * @return Query service pointer
     */
    RequestHandlerServicePtr getRequestHandlerService();

    /**
     * @brief Get instance of query catalog
     * @return query catalog pointer
     */
    Catalogs::Query::QueryCatalogPtr getQueryCatalog();

    /**
     * @brief Return the UDF catalog.
     * @return Pointer to the UDF catalog.
     */
    Catalogs::UDF::UDFCatalogPtr getUDFCatalog();

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
     * getter for the locationService
     * @return
     */
    LocationServicePtr getLocationService() const;

    //Todo #3740: this function is added for testing the fail query request. can be removed once the new request executor is implemented
    /**
     * getter for the global execution plan
     * @return
     */
    Optimizer::GlobalExecutionPlanPtr getGlobalExecutionPlan() const;

    NesWorkerPtr getNesWorker();

    Statistic::StatisticProbeHandlerPtr getStatisticProbeHandler() const;

  private:
    /**
     * @brief this method will start the GRPC Coordinator server which is responsible for reacting to calls from the CoordinatorRPCClient
     */
    void buildAndStartGRPCServer(const std::shared_ptr<std::promise<bool>>& prom);

    CoordinatorConfigurationPtr coordinatorConfiguration;
    std::string restIp;
    uint16_t restPort;
    std::string rpcIp;
    uint16_t rpcPort;
    std::unique_ptr<grpc::Server> rpcServer;
    std::shared_ptr<std::thread> rpcThread;
    NesWorkerPtr worker;
    CoordinatorHealthCheckServicePtr coordinatorHealthCheckService;
    Optimizer::GlobalExecutionPlanPtr globalExecutionPlan;
    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    Catalogs::Query::QueryCatalogPtr queryCatalog;
    TopologyPtr topology;
    RestServerPtr restServer;
    std::shared_ptr<std::thread> restThread;
    std::atomic<bool> isRunning{false};
    RequestHandlerServicePtr requestHandlerService;
    MonitoringServicePtr monitoringService;
    QueryParsingServicePtr queryParsingService;
    RequestQueuePtr queryRequestQueue;
    GlobalQueryPlanPtr globalQueryPlan;
    Catalogs::UDF::UDFCatalogPtr udfCatalog;
    bool enableMonitoring;
    LocationServicePtr locationService;
    Optimizer::PlacementAmendmentHandlerPtr placementAmendmentHandler;
    Statistic::StatisticProbeHandlerPtr statisticProbeHandler;
 uint64_t numberOfBuffersPerEpoch;
 std::shared_ptr<std::thread> statisticOutputThread;

  public:
    constexpr static uint64_t NES_COORDINATOR_ID = 1;
};
using NesCoordinatorPtr = std::shared_ptr<NesCoordinator>;

}// namespace NES
#endif// NES_COORDINATOR_INCLUDE_COMPONENTS_NESCOORDINATOR_HPP_

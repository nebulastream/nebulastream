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

#ifndef NES_CORE_INCLUDE_REST_RESTSERVER_HPP_
#define NES_CORE_INCLUDE_REST_RESTSERVER_HPP_

#include <Runtime/RuntimeForwardRefs.hpp>
#include <condition_variable>
#include <future>
#include <memory>
#include <mutex>
#include <string>

namespace NES {

class RestEngine;
using RestEnginePtr = std::shared_ptr<RestEngine>;

class NesCoordinator;
using NesCoordinatorWeakPtr = std::weak_ptr<NesCoordinator>;

class QueryCatalogService;
using QueryCatalogServicePtr = std::shared_ptr<QueryCatalogService>;

class Topology;
using TopologyPtr = std::shared_ptr<Topology>;

class GlobalExecutionPlan;
using GlobalExecutionPlanPtr = std::shared_ptr<GlobalExecutionPlan>;

class QueryService;
using QueryServicePtr = std::shared_ptr<QueryService>;

class MonitoringService;
using MonitoringServicePtr = std::shared_ptr<MonitoringService>;

class GlobalQueryPlan;
using GlobalQueryPlanPtr = std::shared_ptr<GlobalQueryPlan>;

namespace Catalogs {

namespace Source {
class SourceCatalog;
using SourceCatalogPtr = std::shared_ptr<SourceCatalog>;
}// namespace Source

namespace UDF {
class UdfCatalog;
using UdfCatalogPtr = std::shared_ptr<UdfCatalog>;
}// namespace UDF

}// namespace Catalogs

namespace Experimental {
class MaintenanceService;
using MaintenanceServicePtr = std::shared_ptr<MaintenanceService>;
}//namespace Experimental

namespace Spatial::Index::Experimental {
class LocationService;
using LocationServicePtr = std::shared_ptr<LocationService>;
}// namespace Spatial::Index::Experimental

/**
 * @brief : This class is responsible for starting the REST server.
 */
class RestServer {

  public:
    /**
    * @brief constructor for rest server
    * @param host as string
    * @param port as uint
    * @param handle to coordinator
     *
   * */
    RestServer(std::string host,
               uint16_t port,
               const NesCoordinatorWeakPtr& coordinator,
               const QueryCatalogServicePtr& queryCatalogService,
               const Catalogs::Source::SourceCatalogPtr& sourceCatalog,
               const TopologyPtr& topology,
               const GlobalExecutionPlanPtr& globalExecutionPlan,
               const QueryServicePtr& queryService,
               const MonitoringServicePtr& monitoringService,
               const NES::Experimental::MaintenanceServicePtr& maintenanceService,
               const GlobalQueryPlanPtr& globalQueryPlan,
               const Catalogs::UDF::UdfCatalogPtr& udfCatalog,
               const Runtime::BufferManagerPtr& bufferManager,
               const NES::Spatial::Index::Experimental::LocationServicePtr& locationServicePtr);

    /**
   * @brief method to start the rest server, calls run() internally
   * @return bool indicating success
   */
    bool start();

    /**
   * @brief method called within start()
   * starts the server after initializing controllers, endpoints and necessary components like connection handler, router.
   */
    void run();

    /**
   * @brief method to stop rest server
   * @return bool indicating sucesss
   */
    bool stop();

  private:
    std::string host;
    uint16_t port;
    NesCoordinatorWeakPtr coordinator;
    QueryCatalogServicePtr queryCatalogService;
    GlobalExecutionPlanPtr globalExecutionPlan;
    QueryServicePtr queryService;
    GlobalQueryPlanPtr globalQueryPlan;
    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    TopologyPtr topology;
    Catalogs::UDF::UdfCatalogPtr udfCatalog;
    NES::Spatial::Index::Experimental::LocationServicePtr locationService;
    Experimental::MaintenanceServicePtr maintenanceService;
    MonitoringServicePtr monitoringService;
    Runtime::BufferManagerPtr bufferManager;
    std::condition_variable cvar;
    std::mutex mutex;
    bool stopRequested{false};
};
}// namespace NES

#endif// NES_CORE_INCLUDE_REST_RESTSERVER_HPP_

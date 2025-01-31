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

#ifndef NES_COORDINATOR_INCLUDE_REST_RESTSERVER_HPP_
#define NES_COORDINATOR_INCLUDE_REST_RESTSERVER_HPP_

#include <Runtime/RuntimeForwardRefs.hpp>
#include <condition_variable>
#include <future>
#include <memory>
#include <mutex>
#include <string>

namespace NES {

class NesCoordinator;
using NesCoordinatorWeakPtr = std::weak_ptr<NesCoordinator>;

class QueryCatalogService;
using QueryCatalogServicePtr = std::shared_ptr<QueryCatalogService>;

class Topology;
using TopologyPtr = std::shared_ptr<Topology>;

namespace Optimizer {
class GlobalExecutionPlan;
using GlobalExecutionPlanPtr = std::shared_ptr<GlobalExecutionPlan>;
}// namespace Optimizer

class RequestHandlerService;
using RequestHandlerServicePtr = std::shared_ptr<RequestHandlerService>;

class MonitoringService;
using MonitoringServicePtr = std::shared_ptr<MonitoringService>;

class GlobalQueryPlan;
using GlobalQueryPlanPtr = std::shared_ptr<GlobalQueryPlan>;

class LocationService;
using LocationServicePtr = std::shared_ptr<LocationService>;

class QueryParsingService;
using QueryParsingServicePtr = std::shared_ptr<QueryParsingService>;

namespace Catalogs {

namespace Source {
class SourceCatalog;
using SourceCatalogPtr = std::shared_ptr<SourceCatalog>;
}// namespace Source

namespace UDF {
class UDFCatalog;
using UdfCatalogPtr = std::shared_ptr<UDFCatalog>;
}// namespace UDF

namespace Query {
class QueryCatalog;
using QueryCatalogPtr = std::shared_ptr<QueryCatalog>;
}// namespace Query
}// namespace Catalogs

/**
 * @brief A constant to be used for extending the set of
 * allowed operations without the CORS plugin.
 */
static const std::string corsAllowedMethods = "GET, POST, OPTIONS, DELETE, PUT";

/**
 * @brief : This class is responsible for starting the REST server.
 */
class RestServer {

  public:
    /**
     * @brief constructor for rest server
     * @param host
     * @param port
     * @param coordinator
     * @param queryCatalog
     * @param sourceCatalogService
     * @param topology
     * @param globalExecutionPlan
     * @param requestHandlerService
     * @param monitoringService
     * @param queryParsingService
     * @param globalQueryPlan
     * @param udfCatalog
     * @param bufferManager
     * @param corsAllowedOrigin
     */
    RestServer(std::string host,
               uint16_t port,
               NesCoordinatorWeakPtr coordinator,
               Catalogs::Query::QueryCatalogPtr queryCatalog,
               Catalogs::Source::SourceCatalogPtr sourceCatalogService,
               TopologyPtr topology,
               Optimizer::GlobalExecutionPlanPtr globalExecutionPlan,
               RequestHandlerServicePtr requestHandlerService,
               MonitoringServicePtr monitoringService,
               QueryParsingServicePtr queryParsingService,
               GlobalQueryPlanPtr globalQueryPlan,
               Catalogs::UDF::UDFCatalogPtr udfCatalog,
               Runtime::BufferManagerPtr bufferManager,
               std::optional<std::string> corsAllowedOrigin, CoordinatorConfigurationPtr coordinatorConfiguration
               );

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
    Catalogs::Query::QueryCatalogPtr queryCatalog;
    Optimizer::GlobalExecutionPlanPtr globalExecutionPlan;
    RequestHandlerServicePtr requestHandlerService;
    GlobalQueryPlanPtr globalQueryPlan;
    Catalogs::Source::SourceCatalogPtr sourceCatalogService;
    TopologyPtr topology;
    Catalogs::UDF::UDFCatalogPtr udfCatalog;
    MonitoringServicePtr monitoringService;
    QueryParsingServicePtr queryParsingService;
    Runtime::BufferManagerPtr bufferManager;
    std::condition_variable conditionVariable;
    std::mutex mutex;
    std::optional<std::string> corsAllowedOrigin;
    CoordinatorConfigurationPtr coordinatorConfiguration;
    bool stopRequested{false};
};
}// namespace NES

#endif// NES_COORDINATOR_INCLUDE_REST_RESTSERVER_HPP_

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

#ifndef NES_INCLUDE_REST_RESTENGINE_HPP_
#define NES_INCLUDE_REST_RESTENGINE_HPP_

#if defined(__APPLE__) || defined(__MACH__)
#include <xlocale.h>
#endif
#include <REST/Controller/BaseController.hpp>
#include <REST/CpprestForwardedRefs.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <cpprest/http_listener.h>
#include <pplx/pplxtasks.h>
#include <string>

namespace NES {
class MonitoringService;
using MonitoringServicePtr = std::shared_ptr<MonitoringService>;

class MaintenanceService;
using MaintenanceServicePtr = std::shared_ptr<MaintenanceService> ;

class SourceCatalog;
using SourceCatalogPtr = std::shared_ptr<SourceCatalog>;

class GlobalExecutionPlan;
using GlobalExecutionPlanPtr = std::shared_ptr<GlobalExecutionPlan>;

class QueryService;
using QueryServicePtr = std::shared_ptr<QueryService>;

class GlobalQueryPlan;
using GlobalQueryPlanPtr = std::shared_ptr<GlobalQueryPlan>;

class QueryController;
using QueryControllerPtr = std::shared_ptr<QueryController>;

class QueryCatalogController;
using QueryCatalogControllerPtr = std::shared_ptr<QueryCatalogController>;

class StreamCatalogController;
using StreamCatalogControllerPtr = std::shared_ptr<StreamCatalogController>;

class ConnectivityController;
using ConnectivityControllerPtr = std::shared_ptr<ConnectivityController>;

class MonitoringController;
using MonitoringControllerPtr = std::shared_ptr<MonitoringController>;

class MaintenanceController;
typedef std::shared_ptr<MaintenanceController> MaintenanceControllerPtr;

class TopologyController;
using TopologyControllerPtr = std::shared_ptr<TopologyController>;

class QueryCatalog;
using QueryCatalogPtr = std::shared_ptr<QueryCatalog>;

class NesCoordinator;
using NesCoordinatorWeakPtr = std::weak_ptr<NesCoordinator>;

class Topology;
using TopologyPtr = std::shared_ptr<Topology>;

namespace Catalogs {
class UdfCatalog;
using UdfCatalogPtr = std::shared_ptr<UdfCatalog>;
}// namespace Catalogs

class UdfCatalogController;
using UdfCatalogControllerPtr = std::shared_ptr<UdfCatalogController>;

class RestEngine {

  public:
    RestEngine(const SourceCatalogPtr& streamCatalog,
               const NesCoordinatorWeakPtr& coordinator,
               const QueryCatalogPtr& queryCatalog,
               const TopologyPtr& topology,
               const GlobalExecutionPlanPtr& globalExecutionPlan,
               const QueryServicePtr& queryService,
               const MonitoringServicePtr& monitoringService,
               const MaintenanceServicePtr& maintenanceService,
               const GlobalQueryPlanPtr& globalQueryPlan,
               const Catalogs::UdfCatalogPtr& udfCatalog,
               const Runtime::BufferManagerPtr bufferManager);

    void handleGet(web::http::http_request request);
    void handlePost(web::http::http_request request);
    void handleDelete(web::http::http_request request);
    void handlePut(web::http::http_request request);
    void handlePatch(web::http::http_request request);
    void handleHead(web::http::http_request request);
    void handleTrace(web::http::http_request request);

    /**
    * @brief handle preflight request (complex HTTP request)
    * @param request: the request issued by the client
    * @description A preflight request is sent as a response to a DELETE request,
    *              the response allows the DELETE fetch request of our UI(localhost:3000)
    */
    static void handlePreflightOptions(const web::http::http_request& request);
    void handleMerge(web::http::http_request request);
    void initRestOpHandlers();
    void setEndpoint(const std::string& value);
    [[nodiscard]] std::string endpoint() const;
    pplx::task<void> accept();
    pplx::task<void> shutdown();
    static std::vector<utility::string_t> splitPath(const utility::string_t& path);

    static pplx::task<void> returnDefaultNotImplementedReply(const web::http::method& method, web::http::http_request& request);

    static web::json::value responseNotImpl(const web::http::method& method, utility::string_t path);

    /**
        * @brief Get the URI path from the request
        * @param request : the user request
        * @return the path from the request
    */
    static utility::string_t getPath(web::http::http_request& request);

    /**
    * @brief creates a UnknownEndpointResponse using BadRequestImpl
    */
    static void returnDefaultUnknownEndpointResponse(web::http::http_request request);

  protected:
    web::http::experimental::listener::http_listener _listener;// main micro service network endpoint

  private:
    QueryControllerPtr queryController;
    QueryCatalogControllerPtr queryCatalogController;
    StreamCatalogControllerPtr streamCatalogController;
    ConnectivityControllerPtr connectivityController;
    MonitoringControllerPtr monitoringController;
    TopologyControllerPtr topologyController;
    UdfCatalogControllerPtr udfCatalogController;
    MaintenanceControllerPtr maintenanceController;
};

using RestEnginePtr = std::shared_ptr<RestEngine>;
}// namespace NES
#endif  // NES_INCLUDE_REST_RESTENGINE_HPP_

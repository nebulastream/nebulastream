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

#include <REST/RestServer.hpp>

#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/UDF/UdfCatalog.hpp>
#include <REST/RestEngine.hpp>
#include <Util/Logger/Logger.hpp>
#include <iostream>
#include <oatpp/core/base/Environment.hpp>
#include "oatpp/web/server/HttpConnectionHandler.hpp"
#include "oatpp/network/Server.hpp"
#include "oatpp/network/tcp/server/ConnectionProvider.hpp"


#include <Components/NesCoordinator.hpp>
#include <utility>

namespace NES {

RestServer::RestServer(std::string host,
                       uint16_t port,
                       const NesCoordinatorWeakPtr& coordinator,
                       const QueryCatalogServicePtr& queryCatalogService,
                       const SourceCatalogPtr& sourceCatalog,
                       const TopologyPtr& topology,
                       const GlobalExecutionPlanPtr& globalExecutionPlan,
                       const QueryServicePtr& queryService,
                       const MonitoringServicePtr& monitoringService,
                       const Experimental::MaintenanceServicePtr& maintenanceService,
                       const GlobalQueryPlanPtr& globalQueryPlan,
                       const Catalogs::UdfCatalogPtr& udfCatalog,
                       const Runtime::BufferManagerPtr& bufferManager,
                       const Spatial::Index::Experimental::LocationServicePtr& locationService)
    : restEngine(std::make_shared<RestEngine>(sourceCatalog,
                                              coordinator,
                                              queryCatalogService,
                                              topology,
                                              globalExecutionPlan,
                                              queryService,
                                              monitoringService,
                                              maintenanceService,
                                              globalQueryPlan,
                                              udfCatalog,
                                              bufferManager,
                                              locationService)),
      host(std::move(host)), port(port) {}

bool RestServer::start() {
    NES_DEBUG("RestServer: starting on " << host << ":" << std::to_string(port));
    RestServerInterruptHandler::hookUserInterruptHandler();
    //restEngine->setEndpoint("http://" + host + ":" + std::to_string(port) + "/v1/nes/");
    try {
#ifdef NES_USE_OATPP
        oatpp::base::Environment::init();
        /* Run App */
        run();
        NES_DEBUG("RestServer: after waitForUserInterrupt");
        RestServerInterruptHandler::waitForUserInterrupt();
        /* Destroy oatpp Environment */
        oatpp::base::Environment::destroy();
        restEngine.reset();
#else
        // wait for server initialization...
        auto task = restEngine->accept();
        NES_DEBUG("RestServer: REST Server now listening for requests at: " << restEngine->endpoint());
        task.wait();
        {
            std::unique_lock lock(mutex);
            while (!stopRequested) {
                cvar.wait(lock);
            }
        }
        restEngine.reset();
        shutdownPromise.set_value(true);
        NES_DEBUG("RestServer: after waitForUserInterrupt");
#endif
    } catch (const std::exception& e) {
        NES_ERROR("RestServer: Unable to start REST server << [" << host + ":" + std::to_string(port) << "] " << e.what());
        shutdownPromise.set_exception(std::make_exception_ptr(e));
        return false;
    } catch (...) {
        NES_FATAL_ERROR("RestServer: Unable to start REST server unknown exception.");
        shutdownPromise.set_exception(std::current_exception());
        return false;
    }
    return true;
}


bool RestServer::stop() {
    NES_DEBUG("RestServer::stop");
    auto task = restEngine->shutdown();
    task.wait();
    {
        std::unique_lock lock(mutex);
        stopRequested = true;
        cvar.notify_all();
    }
    return shutdownPromise.get_future().get();
}
void RestServer::run() {
    /* Create Router for HTTP requests routing */
    auto router = oatpp::web::server::HttpRouter::createShared();

    /* Create HTTP connection handler with router */
    auto connectionHandler = oatpp::web::server::HttpConnectionHandler::createShared(router);

    /* Create TCP connection provider */
    auto connectionProvider = oatpp::network::tcp::server::ConnectionProvider::createShared({"localhost", 8000, oatpp::network::Address::IP_4});

    /* Create server which takes provided TCP connections and passes them to HTTP connection handler */
    oatpp::network::Server server(connectionProvider, connectionHandler);

    /* Print info about server port */
    OATPP_LOGI("MyApp", "Server running on port %s", connectionProvider->getProperty("port").getData());

    /* Run server */
    server.run();
}
}// namespace NES

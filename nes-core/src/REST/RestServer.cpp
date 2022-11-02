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

#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/UDF/UdfCatalog.hpp>
#include <Components/NesCoordinator.hpp>
#include <REST/Handlers/ErrorHandler.hpp>
#include <REST/OatppController/ConnectivityController.hpp>
#include <REST/OatppController/LocationController.hpp>
#include <REST/OatppController/MaintenanceController.hpp>
#include <REST/OatppController/QueryCatalogController.hpp>
#include <REST/OatppController/QueryController.hpp>
#include <REST/OatppController/SourceCatalogController.hpp>
#include <REST/OatppController/TopologyController.hpp>
#include <REST/OatppController/UdfCatalogController.hpp>
#include <REST/RestEngine.hpp>
#include <REST/RestServer.hpp>
#include <REST/RestServerInterruptHandler.hpp>
#include <Util/Logger/Logger.hpp>
#include <iostream>
#include <oatpp/network/Address.hpp>
#include <oatpp/network/Server.hpp>
#include <oatpp/network/tcp/server/ConnectionProvider.hpp>
#include <oatpp/parser/json/mapping/ObjectMapper.hpp>
#include <oatpp/web/server/HttpConnectionHandler.hpp>
#include <oatpp/web/server/HttpRouter.hpp>
#include <utility>

namespace NES {

RestServer::RestServer(std::string host,
                       uint16_t port,
                       const NesCoordinatorWeakPtr& coordinator,
                       const QueryCatalogServicePtr& queryCatalogService,
                       const Catalogs::Source::SourceCatalogPtr& sourceCatalog,
                       const TopologyPtr& topology,
                       const GlobalExecutionPlanPtr& globalExecutionPlan,
                       const QueryServicePtr& queryService,
                       const MonitoringServicePtr& monitoringService,
                       const Experimental::MaintenanceServicePtr& maintenanceService,
                       const GlobalQueryPlanPtr& globalQueryPlan,
                       const Catalogs::UDF::UdfCatalogPtr& udfCatalog,
                       const Runtime::BufferManagerPtr& bufferManager,
                       const NES::Spatial::Index::Experimental::LocationServicePtr& locationService)
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
      host(std::move(host)), port(port), coordinator(coordinator), queryCatalogService(queryCatalogService),
      globalExecutionPlan(globalExecutionPlan), queryService(queryService), globalQueryPlan(globalQueryPlan),
      sourceCatalog(sourceCatalog), topology(topology), udfCatalog(udfCatalog), locationService(locationService),
      maintenanceService(maintenanceService) {}

bool RestServer::start(bool useOatpp) {
    if (useOatpp == true) {
        usingOatpp = true;
        return startWithOatpp();
    }
    return startWithRestSDK();
}

bool RestServer::startWithOatpp() {
    NES_INFO("Starting Oatpp Server on " << host << ":" << std::to_string(port));
    RestServerInterruptHandler::hookUserInterruptHandler();
    try {
        // Initialize Oatpp Environment
        oatpp::base::Environment::init();
        //Creates component parts necessary to start server, then starts a blocking server
        run();
        // Destroy Oatpp Environment
        oatpp::base::Environment::destroy();
    } catch (const std::exception& e) {
        NES_ERROR("RestServer: Unable to start REST server << [" << host + ":" + std::to_string(port) << "] " << e.what());
        return false;
    } catch (...) {
        NES_FATAL_ERROR("RestServer: Unable to start REST server unknown exception.");
        return false;
    }
    return true;
}

bool RestServer::startWithRestSDK() {
    NES_DEBUG("RestServer: starting on " << host << ":" << std::to_string(port));
    RestServerInterruptHandler::hookUserInterruptHandler();
    restEngine->setEndpoint("http://" + host + ":" + std::to_string(port) + "/v1/nes/");
    try {
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
    if (!usingOatpp) {
        NES_DEBUG("RestServer::stop");
        auto task = restEngine->shutdown();
        task.wait();
        {
            std::unique_lock lock(mutex);
            stopRequested = true;
            cvar.notify_all();
        }
        return shutdownPromise.get_future().get();
    } else {
        std::unique_lock lock(mutex);
        stopRequested = true;
        return stopRequested;
    }
}

void RestServer::run() {

    /* create serializer and deserializer configurations */
    auto serializeConfig = oatpp::parser::json::mapping::Serializer::Config::createShared();
    auto deserializeConfig = oatpp::parser::json::mapping::Deserializer::Config::createShared();

    /* enable beautifier */
    serializeConfig->useBeautifier = true;

    /* Initialize Object mapper */
    auto objectMapper = oatpp::parser::json::mapping::ObjectMapper::createShared(serializeConfig, deserializeConfig);

    /* Initialize Error Handler */
    ErrorHandlerPtr errorHandler = std::make_shared<ErrorHandler>(objectMapper);

    /* Create Router for HTTP requests routing */
    auto router = oatpp::web::server::HttpRouter::createShared();

    /* Create controllers and add all of their endpoints to the router */
    auto connectivityController = REST::Controller::ConnectivityController::create(objectMapper, "/connectivity");
    auto queryCatalogController = REST::Controller::QueryCatalogController::create(objectMapper,
                                                                                   queryCatalogService,
                                                                                   coordinator,
                                                                                   globalQueryPlan,
                                                                                   "/queryCatalog",
                                                                                   errorHandler);
    auto topologyController = REST::Controller::TopologyController::create(objectMapper, topology, "/topology", errorHandler);
    auto queryController = REST::Controller::QueryController::create(objectMapper,
                                                                     queryService,
                                                                     queryCatalogService,
                                                                     globalExecutionPlan,
                                                                     "/query",
                                                                     errorHandler);
    auto udfCatalogController =
        REST::Controller::UdfCatalogController::create(objectMapper, udfCatalog, "/udfCatalog", errorHandler);
    auto sourceCatalogController =
        REST::Controller::SourceCatalogController::create(objectMapper, sourceCatalog, errorHandler, "/sourceCatalog");
    auto maintenanceController =
        REST::Controller::MaintenanceController::create(objectMapper, maintenanceService, errorHandler, "/maintenance");
    auto locationController =
        REST::Controller::LocationController::create(objectMapper, locationService, "/location", errorHandler);

    router->addController(connectivityController);
    router->addController(queryCatalogController);
    router->addController(queryController);
    router->addController(topologyController);
    router->addController(sourceCatalogController);
    router->addController(udfCatalogController);
    router->addController(maintenanceController);
    router->addController(locationController);

    /* Create HTTP connection handler with router */
    auto connectionHandler = oatpp::web::server::HttpConnectionHandler::createShared(router);
    //register error handler
    connectionHandler->setErrorHandler(std::make_shared<ErrorHandler>(objectMapper));

    /* Create TCP connection provider */
    auto connectionProvider =
        oatpp::network::tcp::server::ConnectionProvider::createShared({host, port, oatpp::network::Address::IP_4});

    /* Create a server, which takes provided TCP connections and passes them to HTTP connection handler. */
    oatpp::network::Server server(connectionProvider, connectionHandler);

    /* Print info about server port */
    NES_INFO("NebulaStream REST Server listening on port " << port);

    /* Run server */
    server.run([this]() -> bool {
        bool run = true;
        {
            NES_DEBUG("checking if stop request has arrived for rest server");
            std::unique_lock lock(mutex);
            if (stopRequested) {
                run = false;
            }
        }
        NES_DEBUG("Should server continue running: " << run);
        return run;
    });
}

}// namespace NES

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

#include <REST/RestServer.hpp>

#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/UDF/UdfCatalog.hpp>
#include <REST/RestEngine.hpp>
#include <REST/runtime_utils.hpp>
#include <REST/usr_interrupt_handler.hpp>
#include <Util/Logger.hpp>
#include <iostream>

#include <Components/NesCoordinator.hpp>
#include <utility>

namespace NES {

RestServer::RestServer(std::string host,
                       uint16_t port,
                       const NesCoordinatorWeakPtr& coordinator,
                       const QueryCatalogPtr& queryCatalog,
                       const SourceCatalogPtr& streamCatalog,
                       const TopologyPtr& topology,
                       const GlobalExecutionPlanPtr& globalExecutionPlan,
                       const QueryServicePtr& queryService,
                       const MonitoringServicePtr& monitoringService,
                       const MaintenanceServicePtr& maintenanceService,
                       const GlobalQueryPlanPtr& globalQueryPlan,
                       const Catalogs::UdfCatalogPtr& udfCatalog,
                       const Runtime::BufferManagerPtr& bufferManager)
    : restEngine(std::make_shared<RestEngine>(streamCatalog,
                                              coordinator,
                                              queryCatalog,
                                              topology,
                                              globalExecutionPlan,
                                              queryService,
                                              monitoringService,
                                              maintenanceService,
                                              globalQueryPlan,
                                              udfCatalog,
                                              bufferManager)),
      host(std::move(host)), port(port) {}

bool RestServer::start() {

    NES_DEBUG("RestServer: starting on " << host << ":" << std::to_string(port));
    InterruptHandler::hookUserInterruptHandler();
    restEngine->setEndpoint("http://" + host + ":" + std::to_string(port) + "/v1/nes/");
    try {
        // wait for server initialization...
        restEngine->accept().wait();
        NES_DEBUG("RestServer: Server started");
        NES_DEBUG("RestServer: REST Server now listening for requests at: " << restEngine->endpoint());
        InterruptHandler::waitForUserInterrupt();
        restEngine->shutdown();
        restEngine.reset();
        NES_DEBUG("RestServer: after waitForUserInterrupt");
    } catch (const std::exception& e) {
        NES_ERROR("RestServer: Unable to start REST server << " << e.what());
        return false;
    } catch (...) {
        RuntimeUtils::printStackTrace();
        return false;
    }
    return true;
}

bool RestServer::stop() {
    NES_DEBUG("RestServer::stop");
    InterruptHandler::handleUserInterrupt(SIGTERM);
    return true;
}

}// namespace NES

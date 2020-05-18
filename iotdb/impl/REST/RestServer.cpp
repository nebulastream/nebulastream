#include <REST/RestServer.hpp>
#include <REST/runtime_utils.hpp>
#include <REST/usr_interrupt_handler.hpp>
#include <Util/Logger.hpp>
#include <iostream>
#include <Catalogs/StreamCatalog.hpp>

namespace NES {

RestServer::RestServer(std::string host, u_int16_t port,
                       NesCoordinatorPtr coordinator,
                       QueryCatalogPtr queryCatalog,
                       StreamCatalogPtr streamCatalog,
                       TopologyManagerPtr topologyManager) {
    this->host = host;
    this->port = port;
    this->coordinator = coordinator;
    this->queryCatalog = queryCatalog;
    restEngine = std::make_shared<RestEngine>(streamCatalog, coordinator, queryCatalog, topologyManager);
    InterruptHandler::hookSIGINT();
}

RestServer::~RestServer() {
    NES_DEBUG("~RestServer");
}

bool RestServer::start() {

    NES_DEBUG("RestServer: starting on " << host << ":" << std::to_string(port));

    restEngine->setEndpoint("http://" + host + ":" + std::to_string(port) + "/v1/nes/");
    try {
        // wait for server initialization...
        restEngine->accept().wait();
        NES_DEBUG("RestServer: Server started");
        NES_DEBUG("RestServer: REST Server now listening for requests at: " << restEngine->endpoint());
        InterruptHandler::waitForUserInterrupt();
        restEngine->shutdown();
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
    InterruptHandler::handleUserInterrupt(SIGINT);
    return true;
}

}// namespace NES

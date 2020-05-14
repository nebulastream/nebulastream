#include <REST/RestServer.hpp>
#include <REST/runtime_utils.hpp>
#include <REST/usr_interrupt_handler.hpp>
#include <Util/Logger.hpp>
#include <iostream>

namespace NES {

RestServer::RestServer(std::string host, u_int16_t port,
                       NesCoordinatorPtr coordinator) {
    this->host = host;
    this->port = port;
    this->coordinator = coordinator;
    InterruptHandler::hookSIGINT();
}

RestServer::~RestServer() {
    NES_DEBUG("~RestServer");
}

bool RestServer::start() {

    NES_DEBUG("RestServer: starting on " << host << ":" << std::to_string(port));

    server.setCoordinator(coordinator);
    server.setEndpoint("http://" + host + ":" + std::to_string(port) + "/v1/nes/");

    try {
        // wait for server initialization...
        server.accept().wait();
        NES_DEBUG("RestServer: Server started");
        NES_DEBUG("RestServer: REST Server now listening for requests at: " << server.endpoint());
        InterruptHandler::waitForUserInterrupt();
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
    //    server.shutdown().wait();
    return true;
}

}// namespace NES

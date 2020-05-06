#include <REST/RestServer.hpp>
#include <REST/runtime_utils.hpp>
#include <REST/usr_interrupt_handler.hpp>
#include <Util/Logger.hpp>
#include <iostream>

namespace NES {

RestServer::RestServer(std::string host, u_int16_t port,
                       infer_handle_from_class_t<CoordinatorActor> coordinatorActorHandle) {
    this->host = host;
    this->port = port;
    this->coordinatorActorHandle = coordinatorActorHandle;
}

bool RestServer::start() {

    NES_DEBUG("RestServer: starting on " << host << ":" << std::to_string(port));

    server.setCoordinatorActorHandle(coordinatorActorHandle);
    server.setEndpoint("http://" + host + ":" + std::to_string(port) + "/v1/nes/");

    try {
        // wait for server initialization...
        server.accept().wait();
        NES_DEBUG("RestServer: Server started");
        NES_DEBUG("RestServer: REST Server now listening for requests at: " << server.endpoint());
        InterruptHandler::waitForUserInterrupt();
        NES_DEBUG("RestServer: after waitForUserInterrupt");
    } catch (std::exception& e) {
        NES_ERROR("RestServer: Unable to start REST server");
        return false;
    } catch (...) {
        RuntimeUtils::printStackTrace();
        return false;
    }
    return true;
}

bool RestServer::stop() {
    InterruptHandler::handleUserInterrupt(SIGINT);
    return true;
}

}// namespace NES

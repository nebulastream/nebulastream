#include <iostream>
#include <REST/usr_interrupt_handler.hpp>
#include <REST/runtime_utils.hpp>
#include <REST/RestServer.hpp>
#include <Util/Logger.hpp>

namespace NES{

RestServer::RestServer(std::string host, u_int16_t port,
           infer_handle_from_class_t<CoordinatorActor> coordinatorActorHandle)
{
  this->host = host;
  this->port = port;
  this->coordinatorActorHandle = coordinatorActorHandle;
}


bool RestServer::start() {

    NES_DEBUG("RestServer: REST server set to host " << host << " port" << port)

    server.setCoordinatorActorHandle(coordinatorActorHandle);
    server.setEndpoint("http://" + host + ":" + std::to_string(port) + "/v1/nes/");

    try {
        // wait for server initialization...
        server.accept().wait();
        NES_DEBUG("RestServer: Server started")
        NES_DEBUG("RestServer: REST Server now listening for requests at: " << server.endpoint())
        InterruptHandler::waitForUserInterrupt();
        NES_DEBUG("RestServer: after waitForUserInterrupt")
//        server.shutdown().wait();
    } catch (std::exception& e) {
        NES_ERROR("RestServer: Unable to start REST server");
        return false;
    } catch (...) {
        RuntimeUtils::printStackTrace();
        return false;
    }
    return true;
}

bool RestServer::stop()
{
  InterruptHandler::handleUserInterrupt(SIGINT);
  //TODO: should return bool
  return true;
}



}

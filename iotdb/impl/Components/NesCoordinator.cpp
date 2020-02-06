#include <Components/NesCoordinator.hpp>
#include <Util/Logger.hpp>
#include <caf/io/all.hpp>
#include <thread>

namespace NES {

NesCoordinator::NesCoordinator() {
  restPort = 8081;
  restHost = "localhost";
}

infer_handle_from_class_t<CoordinatorActor> NesCoordinator::getActorHandle() {
  return coordinatorActorHandle;
}

void startRestServer(
    RestServer *restServer, std::string restHost, uint16_t restPort,
    infer_handle_from_class_t<CoordinatorActor> coordinatorActorHandle) {

  restServer = new RestServer(restHost, restPort, coordinatorActorHandle);
  restServer->start();
}

void starter(infer_handle_from_class_t<CoordinatorActor> handle,
             actor_system *actorSystem, CAFServer *cafServer,
             RestServer *restServer, std::string restHost, uint16_t restPort) {
  CoordinatorActorConfig actorCoordinatorConfig;
  actorCoordinatorConfig.load<io::middleman>();
  handle = actorSystem->spawn<CoordinatorActor>();
  NES_DEBUG("NesCoordinator: actor handle created")

  NES_DEBUG("NesCoordinator start caf server thread")
  cafServer = new CAFServer(handle, actorSystem);
  cafServer->start();
  NES_DEBUG("NesCoordinator start caf actor and server successfully")

  restServer = new RestServer(restHost, restPort, handle);
  restServer->start();
  NES_DEBUG("NesCoordinator start rest actor and server successfully")

}

void NesCoordinator::stopCoordinator() {
  restServer->stop();
  cafServer->stop();
  restServerThread.join();
  actorThread.join();
}

bool NesCoordinator::startCoordinator(bool blocking) {
  NES_DEBUG("NesCoordinator: Start Rest Server")

  NES_DEBUG("NesCoordinator start")
  actorCoordinatorConfig.load<io::middleman>();

  actorSystem = new actor_system { actorCoordinatorConfig };

  std::thread th0(starter, coordinatorActorHandle, actorSystem, cafServer,
                  restServer, restHost, restPort);
  actorThread = std::move(th0);

  if (blocking) {
    NES_DEBUG("NesCoordinator started, join now and waiting for work")
//    restServerThread.join();
    actorThread.join();
  } else {
    NES_DEBUG("NesCoordinator started, return without blocking")
    return true;
  }
}

void NesCoordinator::setRestConfiguration(std::string host, uint16_t port) {
  this->restHost = restHost;
  this->restPort = port;
}

}

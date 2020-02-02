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

void startActor(infer_handle_from_class_t<CoordinatorActor> handle,
                actor_system *actorSystem) {
  CoordinatorActorConfig actorCoordinatorConfig;
  actorCoordinatorConfig.load<io::middleman>();
  handle = actorSystem->spawn<CoordinatorActor>();
  NES_DEBUG("NesCoordinator: actor handle created")

  NES_DEBUG("NesCoordinator start caf server thread")
  CAFServer *cafServer;

  cafServer = new CAFServer(handle, actorSystem);
  cafServer->start();

  NES_DEBUG("NesCoordinator start caf actor and server successfully")
}

void NesCoordinator::stopCoordinator() {
  restServer->stop();
  cafServer->stop();
  restServerThread.join();
  actorThread.join();
}

bool NesCoordinator::startCoordinator(bool blocking) {
  NES_DEBUG("NesCoordinator: Start Rest Server")

  NES_DEBUG("NesCoordinator start actor ")
//  CoordinatorActorConfig actorCoordinatorConfig;
  actorCoordinatorConfig.load<io::middleman>();

  actorSystem = new actor_system { actorCoordinatorConfig };
//  coordinatorActorHandle = actorSystem->spawn<CoordinatorActor>();

  std::thread th0(startActor, coordinatorActorHandle, actorSystem);
  actorThread = std::move(th0);

  NES_DEBUG("NesCoordinator start rest server thread")
  std::thread th1(startRestServer, restServer, restHost, restPort,
                  coordinatorActorHandle);
  restServerThread = std::move(th1);

  if (blocking) {
    NES_DEBUG("NesCoordinator started, join now and waiting for work")
    restServerThread.join();
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

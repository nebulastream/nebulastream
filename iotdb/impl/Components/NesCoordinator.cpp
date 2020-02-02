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

void startCafServer(
    CAFServer *cafServer,
    infer_handle_from_class_t<CoordinatorActor> coordinatorActorHandle) {
  cafServer = new CAFServer(coordinatorActorHandle);
  cafServer->start();
}

void startActor(infer_handle_from_class_t<CoordinatorActor> handle)
{
  CoordinatorActorConfig actorCoordinatorConfig;
  actorCoordinatorConfig.load<io::middleman>();
  actor_system actorSystem { actorCoordinatorConfig };
  handle = actorSystem.spawn<CoordinatorActor>();
}

void NesCoordinator::stopCoordinator() {
  //TODO: makes no sense currently as start coordinator is blocking
  NES_NOT_IMPLEMENTED
}

bool NesCoordinator::startCoordinator(bool blocking) {
  NES_DEBUG("NesCoordinator: Start Rest Server")

  NES_DEBUG("NesCoordinator start actor ")
  CoordinatorActorConfig actorCoordinatorConfig;
  actorCoordinatorConfig.load<io::middleman>();
  actor_system actorSystem { actorCoordinatorConfig };

  coordinatorActorHandle = actorSystem.spawn<CoordinatorActor>();
//  std::thread th0(startActor, coordinatorActorHandle);
//  startActor(coordinatorActorHandle);
//  actorThread = std::move(th0);

  NES_DEBUG("NesCoordinator start rest server thread")
  std::thread th1(startRestServer, restServer, restHost, restPort,
                  coordinatorActorHandle);
  restServerThread = std::move(th1);

  NES_DEBUG("NesCoordinator start caf server thread")
  std::thread th2(startCafServer, cafServer, coordinatorActorHandle);
  cafServerThread = std::move(th2);

  if (blocking) {
    NES_DEBUG("NesCoordinator started, join now and waiting for work")
    restServerThread.join();
    cafServerThread.join();
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

#include <Components/NesCoordinator.hpp>
#include <Util/Logger.hpp>
#include <caf/io/all.hpp>
#include <thread>

namespace NES {

NesCoordinator::NesCoordinator() {
  restPort = 8081;
  restHost = "localhost";
  publishPort = 0;

}

infer_handle_from_class_t<CoordinatorActor> NesCoordinator::getActorHandle() {
  return coordinatorActorHandle;
}

void starter(infer_handle_from_class_t<CoordinatorActor> handle,
             actor_system *actorSystem, RestServer *restServer,
             std::string restHost, uint16_t restPort, size_t* port) {
  CoordinatorActorConfig actorCoordinatorConfig;
  actorCoordinatorConfig.load<io::middleman>();
  handle = actorSystem->spawn<CoordinatorActor>();
  NES_DEBUG("NesCoordinator: actor handle created")

  NES_DEBUG(
      "NesCoordinator: trying to publish at port " << *port)

  io::unpublish(handle, actorCoordinatorConfig.publish_port);

  auto expected_port = io::publish(handle, 0,
                                   nullptr, true);
  if (!expected_port) {
    NES_ERROR("NesCoordinator: publish failed: " << expected_port)
    throw new Exception("NesCoordinator start failed");
  }
  NES_DEBUG(
      "NesCoordinator: coordinator successfully published at port " << expected_port)

  *port = *expected_port;
  restServer = new RestServer(restHost, restPort, handle);
  restServer->start();
  NES_DEBUG("NesCoordinator: starter thread terminates")
}

void NesCoordinator::stopCoordinator() {
  scoped_actor self { *actorSystem };
  self->request(coordinatorActorHandle, task_timeout,
                exit_reason::user_shutdown);
  NES_DEBUG("NesCoordinator: shutdown sended to coordinator")
  restServer->stop();
  NES_DEBUG("NesCoordinator: rest server stopped")

  actorThread.join();
  NES_DEBUG("NesCoordinator: thread joined")
}

size_t NesCoordinator::startCoordinator(bool blocking) {
  NES_DEBUG("NesCoordinator: Start Rest Server")

  NES_DEBUG("NesCoordinator start")
  actorCoordinatorConfig.load<io::middleman>();

  actorSystem = new actor_system { actorCoordinatorConfig };

  std::thread th0(starter, coordinatorActorHandle, actorSystem, restServer,
                  restHost, restPort, &publishPort);
  actorThread = std::move(th0);

  if (blocking) {
    NES_DEBUG("NesCoordinator started, join now and waiting for work")
    actorThread.join();
  } else {
    NES_DEBUG("NesCoordinator started, return without blocking on port " << publishPort)
    return publishPort;
  }
}

void NesCoordinator::setRestConfiguration(std::string host, uint16_t port) {
  this->restHost = restHost;
  this->restPort = port;
}

}

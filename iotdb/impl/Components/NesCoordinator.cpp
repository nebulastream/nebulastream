#include <Components/NesCoordinator.hpp>
#include <Util/Logger.hpp>
#include <caf/io/all.hpp>
#include <thread>

namespace NES {

NesCoordinator::NesCoordinator() {
  restPort = 8081;
  restHost = "localhost";
  actorPort = 0;

}

infer_handle_from_class_t<CoordinatorActor> NesCoordinator::getActorHandle() {
  return coordinatorActorHandle;
}

void starter(infer_handle_from_class_t<CoordinatorActor> handle,
             actor_system *actorSystem, RestServer *restServer,
             std::string restHost, uint16_t restPort, uint16_t *port) {
  restServer = new RestServer(restHost, restPort, handle);
  restServer->start();  //this call is blocking
  NES_DEBUG("NesCoordinator: starter thread terminates")
}

bool NesCoordinator::stopCoordinator() {
  scoped_actor self { *actorSystem };
  self->request(coordinatorActorHandle, task_timeout,
                exit_reason::user_shutdown);
  NES_DEBUG("NesCoordinator: shutdown sended to coordinator")
  bool retStopRest = restServer->stop();
  NES_DEBUG("NesCoordinator: rest server stopped")

  restThread.join();
  NES_DEBUG("NesCoordinator: thread joined")
  return retStopRest;
}

void NesCoordinator::startCoordinator(bool blocking, uint16_t port) {
  actorPort = port;
  startCoordinator(blocking);
}

string NesCoordinator::executeQuery(const string queryString,
                                    const string strategy) {
  NES_DEBUG("NesCoordinator: execute query=" << queryString << " with strategy=" << strategy)

  scoped_actor self { *actorSystem };
  string queryId = "";

  self->request(coordinatorActorHandle, task_timeout, execute_query_atom::value,
                queryString, strategy).receive(
      [&queryId](const string& uuid) mutable {
        queryId = uuid;
        NES_DEBUG("NesCoordinator: query successuflly executed id=" << uuid)
      }
      ,
      [=](const error &er) {
        NES_ERROR("NesCoordinator: query not executed")
        string error_msg = to_string(er);
      });

  return queryId;
}

uint16_t NesCoordinator::startCoordinator(bool blocking) {
  NES_DEBUG("NesCoordinator: Start Rest Server")

  NES_DEBUG("NesCoordinator start")
  actorCoordinatorConfig.load<io::middleman>();

  actorSystem = new actor_system { actorCoordinatorConfig };

  coordinatorActorHandle = actorSystem->spawn<CoordinatorActor>();
  NES_DEBUG("NesCoordinator: actor handle created")

  io::unpublish(coordinatorActorHandle, actorPort);

  auto expected_port = io::publish(coordinatorActorHandle, actorPort, nullptr,
                                   true);
  if (!expected_port) {
    NES_ERROR("NesCoordinator: publish failed: " << expected_port)
    throw new Exception("NesCoordinator start failed");
  }
  NES_DEBUG(
      "NesCoordinator: coordinator successfully published at port " << expected_port)

  actorPort = *expected_port;

  std::thread th0(starter, coordinatorActorHandle, actorSystem, restServer,
                  restHost, restPort, &actorPort);
  restThread = std::move(th0);

  if (blocking) {
    NES_DEBUG("NesCoordinator started, join now and waiting for work")
    restThread.join();
  } else {
    NES_DEBUG(
        "NesCoordinator started, return without blocking on port " << actorPort)
    return actorPort;
  }
}

void NesCoordinator::setRestConfiguration(std::string host, uint16_t port) {
  this->restHost = restHost;
  this->restPort = port;
}

}

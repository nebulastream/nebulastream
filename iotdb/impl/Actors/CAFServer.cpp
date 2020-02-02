#include <iostream>

#include <Actors/CAFServer.hpp>
#include <REST/usr_interrupt_handler.hpp>
#include <Actors/Configurations/CoordinatorActorConfig.hpp>
#include <Util/Logger.hpp>
using namespace caf;

namespace NES {
CAFServer::CAFServer(
    infer_handle_from_class_t<CoordinatorActor> coordinatorActorHandle) {
  this->coordinatorActorHandle = coordinatorActorHandle;
//  actorCoordinatorConfig.load<io::middleman>();
//  actorSystem{actorCoordinatorConfig};

//  setupLogging();
}
bool CAFServer::start() {
  CoordinatorActorConfig actorCoordinatorConfig;
  actorCoordinatorConfig.load<io::middleman>();

  //Prepare Actor System
  actor_system actorSystem { actorCoordinatorConfig };

  NES_DEBUG(
      "CAFServer: trying to publish at port " << actorCoordinatorConfig.publish_port)

  io::unpublish(coordinatorActorHandle, actorCoordinatorConfig.publish_port);

  auto expected_port = io::publish(coordinatorActorHandle,
                                   actorCoordinatorConfig.publish_port);
  if (!expected_port) {
    NES_ERROR("CAFServer: publish failed: " << *expected_port)
    return false;
  }
  NES_DEBUG(
      "CAFServer: coordinator successfully published at port " << *expected_port)

  //TODO: This code is to be migrated when we create CLI based client for interacting with the actor_system
  // defining the handler outside the loop is more efficient as it avoids
  // re-creating the same object over and over again

  return true;
}

bool CAFServer::stop() {
  NES_NOT_IMPLEMENTED
}

}

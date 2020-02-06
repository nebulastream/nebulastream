#include <iostream>

#include <Actors/CAFServer.hpp>
#include <REST/usr_interrupt_handler.hpp>
#include <Actors/Configurations/CoordinatorActorConfig.hpp>
#include <Util/Logger.hpp>
using namespace caf;

namespace NES {
CAFServer::CAFServer(
    infer_handle_from_class_t<CoordinatorActor> coordinatorActorHandle,
    actor_system *acSys) {
  this->coordinatorActorHandle = coordinatorActorHandle;
  this->acSys = acSys;
}

bool CAFServer::start() {
  CoordinatorActorConfig actorCoordinatorConfig;
  actorCoordinatorConfig.load<io::middleman>();

  //Prepare Actor System
//  actor_system actorSystem { actorCoordinatorConfig };

  NES_DEBUG(
      "CAFServer: trying to publish at port " << actorCoordinatorConfig.publish_port)

  io::unpublish(coordinatorActorHandle, actorCoordinatorConfig.publish_port);

  auto expected_port = io::publish(coordinatorActorHandle,
                                   actorCoordinatorConfig.publish_port);
  if (!expected_port) {
    NES_ERROR("CAFServer: publish failed: " << *expected_port)
    throw new Exception("CAFServer start failed");
    return false;//TODO: is it better to return true?
  }
  NES_DEBUG(
      "CAFServer: coordinator successfully published at port " << *expected_port)

  return true;
}

bool CAFServer::stop() {
  CoordinatorActorConfig actorCoordinatorConfig;
  actorCoordinatorConfig.load<io::middleman>();
  io::unpublish(coordinatorActorHandle, actorCoordinatorConfig.publish_port);
}

}

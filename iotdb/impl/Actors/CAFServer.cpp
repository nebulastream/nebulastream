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
  actorCoordinatorConfig.load<io::middleman>();
//  actorSystem{actorCoordinatorConfig};

  setupLogging();
}
bool CAFServer::start() {
  NES_DEBUG(
      "CAFServer: trying to publish at port " << actorCoordinatorConfig.publish_port)


  io::unpublish(coordinatorActorHandle, actorCoordinatorConfig.publish_port);

  auto expected_port = io::publish(coordinatorActorHandle,
                                   actorCoordinatorConfig.publish_port);
  if (!expected_port) {
    NES_ERROR(
        "CAFServer: publish failed: " << actorCoordinatorConfig.render(expected_port.error()))
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

void CAFServer::setupLogging() {

  // create PatternLayout
  log4cxx::LayoutPtr layoutPtr(
      new log4cxx::PatternLayout(
          "%d{MMM dd yyyy HH:mm:ss} %c:%L [%-5t] [%p] : %m%n"));

  // create FileAppender
  LOG4CXX_DECODE_CHAR(fileName, "cafServer.log");
  log4cxx::FileAppenderPtr file(new log4cxx::FileAppender(layoutPtr, fileName));

  // create ConsoleAppender
  log4cxx::ConsoleAppenderPtr console(new log4cxx::ConsoleAppender(layoutPtr));

  NES::NESLogger->setLevel(log4cxx::Level::getDebug());
  NES::NESLogger->addAppender(file);
  NES::NESLogger->addAppender(console);

  // set log level
  log4cxx::Logger::getLogger("NES")->setLevel(log4cxx::Level::getDebug());
}
}

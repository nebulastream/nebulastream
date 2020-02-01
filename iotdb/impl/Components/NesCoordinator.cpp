#include <Components/NesCoordinator.hpp>
#include <Util/Logger.hpp>
#include <caf/io/all.hpp>
#include <thread>

namespace NES {

NesCoordinator::NesCoordinator() {
  restPort = 8081;
  restHost = "localhost";

  actorCoordinatorConfig.load<io::middleman>();

  //Prepare Actor System
  actor_system actorSystem { actorCoordinatorConfig };

  coordinatorActorHandle = actorSystem.spawn<CoordinatorActor>();

  //Start the CAF server
  std::thread t2(startCAFServer, coord);
  std::cout << "CAF server started\n";

  std::thread t1(startRestServer, restPort, restHost, coord);
  std::cout << "REST server started\n";
  t1.join();

  t2.join();
}

void NesCoordinator::startCoordinator()
{
  restServer.start(restHost, restPort, coordinatorActorHandle);
  cafServer.start(coordinatorActorHandle);

}
bool NesCoordinator::startRestServer(
    std::string host, u_int16_t port,
    infer_handle_from_class_t<CoordinatorActor> coordinatorActorHandle) {

  return restServer.start(host, port, coordinatorActorHandle);
}


void NesCoordinator::setRestConfiguration(std::string host, uint16_t port) {
  this->restHost = restHost;
  this->restPort = port;
}

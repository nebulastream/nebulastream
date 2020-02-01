#ifndef INCLUDE_COMPONENTS_NESCOORDINATOR_HPP_
#define INCLUDE_COMPONENTS_NESCOORDINATOR_HPP_
#include "REST/RestServer.hpp"
#include "Actors/CAFServer.hpp"
#include <string>

namespace NES {

class NesCoordinator {
 public:
  /**
   * @brief default constructor
   * @note this will create the coordinator actor using the default coordinator config
   * @limitations
   *    - currently we start rest server always
   */
  NesCoordinator();

  bool startCoordinator();
  void stopCoordinator();

  void setRestConfiguration(std::string host, uint16_t port);

  void startCLI();

 private:
  CoordinatorActorConfig actorCoordinatorConfig;
  infer_handle_from_class_t<CoordinatorActor> coordinatorActorHandle;
  RestServer* restServer;
  CAFServer* cafServer;
  std::string restHost;
  uint16_t restPort;
};
}
#endif /* INCLUDE_COMPONENTS_NESCOORDINATOR_HPP_ */

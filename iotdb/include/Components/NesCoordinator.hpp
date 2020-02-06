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

  /**
   * @brief start actor: rest server, caf server, coordinator actor
   * @param bool if the method should block
   * @note this is a blocking call
   */
  bool startCoordinator(bool blocking);

  /**
   * @brief method to stop coordinator
   */
  void stopCoordinator();

  /**
   * @method to overwrite the default config for the rest server
   * @param host as string
   * @param port as uint
   */
  void setRestConfiguration(std::string host, uint16_t port);

  /**
   * @brief method to get handle
   * @return handle to actor
   */
  infer_handle_from_class_t<CoordinatorActor> getActorHandle();

 private:
  CoordinatorActorConfig actorCoordinatorConfig;
  infer_handle_from_class_t<CoordinatorActor> coordinatorActorHandle;
  actor_system* actorSystem;
  RestServer* restServer;
  CAFServer* cafServer;
  std::string restHost;
  uint16_t restPort;
  std::thread actorThread;
};
typedef std::shared_ptr<NesCoordinator> NesCoordinatorPtr;

}
#endif /* INCLUDE_COMPONENTS_NESCOORDINATOR_HPP_ */

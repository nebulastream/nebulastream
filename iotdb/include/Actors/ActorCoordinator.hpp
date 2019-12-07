#ifndef IOTDB_INCLUDE_ACTORS_ACTORCOORDINATOR_HPP_
#define IOTDB_INCLUDE_ACTORS_ACTORCOORDINATOR_HPP_

#include <caf/all.hpp>
#include <Topology/FogTopologyEntry.hpp>
#include <Services/CoordinatorService.hpp>
#include <Services/WorkerService.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <Actors/Configurations/ActorCoordinatorConfig.hpp>
#include <Topology/FogTopologyManager.hpp>

using namespace caf;
using std::cout;
using std::cerr;
using std::endl;
using std::string;
using std::unordered_map;

namespace iotdb {

// class-based, statically typed, event-based API for the state management in CAF
struct coordinator_state {
  CoordinatorServicePtr coordinatorServicePtr;
  std::unique_ptr<WorkerService> workerServicePtr;

  unordered_map<strong_actor_ptr, FogTopologyEntryPtr> actorTopologyMap;
  unordered_map<FogTopologyEntryPtr, strong_actor_ptr> topologyActorMap;
};

/**
* @brief the coordinator for NES
*/
class actor_coordinator : public stateful_actor<coordinator_state> {

 public:
  /**
  * @brief the constructor of the coordinator to initialize the default objects
  */
  explicit actor_coordinator(actor_config &cfg)
      : stateful_actor(cfg) {
    this->state.coordinatorServicePtr = CoordinatorService::getInstance();
    this->state.workerServicePtr =
        std::make_unique<WorkerService>(WorkerService(actorCoordinatorConfig.ip,
                                                      actorCoordinatorConfig.publish_port,
                                                      actorCoordinatorConfig.receive_port,
                                                      ""));
  }

  behavior_type make_behavior() override {
    return init();
  }

 private:
  behavior init();
  behavior running();

  /**
   * @brief : registering a new sensor node
   *
   * @param ip
   * @param publish_port
   * @param receive_port
   * @param cpu
   * @param sensor
   */
  void register_sensor(const string &ip, uint16_t publish_port, uint16_t receive_port, int cpu, const string &sensor);

  /**
   * @brief: deploy the user query
   *
   * @param queryString
   */
  void deploy_query(const string &queryString);

  /**
   * @brief method which is called to unregister an already running query
   *
   * @param queryId of the query to be de-registered
   */
  void deregister_query(const string &queryId);

  /**
   * @brief send messages to all connected devices and get their operators
   */
  void show_operators();

  /**
   * @brief initialize the NES topology and add coordinator node
   */
  void initializeNESTopology();

  ActorCoordinatorConfig actorCoordinatorConfig;
};

}

#endif //IOTDB_INCLUDE_ACTORS_ACTORCOORDINATOR_HPP_

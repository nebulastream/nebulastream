//
// Created by xchatziliadis on 26.11.19.
//

#ifndef IOTDB_INCLUDE_ACTORS_ACTORCOORDINATOR_HPP_
#define IOTDB_INCLUDE_ACTORS_ACTORCOORDINATOR_HPP_

#include <Topology/FogTopologyEntry.hpp>
#include <Services/CoordinatorService.hpp>
#include <Services/WorkerService.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <caf/all.hpp>

using namespace caf;
using std::cout;
using std::cerr;
using std::endl;
using std::string;
using std::unordered_map;

namespace iotdb {

// class-based, statically typed, event-based API for the state management in CAF
struct coordinator_state {
  std::unique_ptr<CoordinatorService> coordinatorPtr;
  std::unique_ptr<WorkerService> workerPtr;

  unordered_map<strong_actor_ptr, FogTopologyEntryPtr> actorTopologyMap;
  unordered_map<FogTopologyEntryPtr, strong_actor_ptr> topologyActorMap;
};

/**
* @brief the coordinator for NES
*/
class actor_coordinator : public stateful_actor<coordinator_state> {

 public:
  /**
  * @brief the constructior of the coordinator to initialize the default objects
  */
  explicit actor_coordinator(actor_config &cfg, string ip, uint16_t publish_port, uint16_t receive_port)
      : stateful_actor(cfg) {
    this->state.coordinatorPtr =
        std::make_unique<CoordinatorService>(CoordinatorService(ip, publish_port, receive_port));
    this->state.workerPtr = std::make_unique<WorkerService>(WorkerService(ip, publish_port, receive_port, ""));
  }

  behavior_type make_behavior() override {
    return init();
  }

 private:
  behavior init();
  behavior running();

  void register_sensor(const string &ip, uint16_t publish_port, uint16_t receive_port, int cpu, const string &sensor);

  void deploy_query(const string &description);

  /**
   * @brief method which is called to unregister an already running query
   * @param description the description of the query
   */
  void deregister_query(const string &description);

  /**
 * @brief send messages to all connected devices and get their operators
 */
  void show_operators();
};

}

#endif //IOTDB_INCLUDE_ACTORS_ACTORCOORDINATOR_HPP_

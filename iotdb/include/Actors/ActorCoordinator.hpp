//
// Created by xchatziliadis on 26.11.19.
//

#ifndef IOTDB_INCLUDE_ACTORS_ACTORCOORDINATOR_HPP_
#define IOTDB_INCLUDE_ACTORS_ACTORCOORDINATOR_HPP_

#include <Topology/FogTopologyEntry.hpp>
#include <Actors/NesCoordinator.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <caf/all.hpp>

using namespace caf;
using std::cout;
using std::cerr;
using std::endl;
using std::string;
using std::unordered_map;

namespace iotdb {
constexpr auto task_timeout = std::chrono::seconds(10);

/**
* @brief The configuration file of the coordinator
*/
class coordinator_config : public actor_system_config {
 public:
  std::string ip = "127.0.0.1";
  uint16_t publish_port = 4711;
  uint16_t receive_port = 4815;

  coordinator_config() {
    opt_group{custom_options_, "global"}
        .add(ip, "ip", "set ip")
        .add(publish_port, "publish_port,ppub", "set publish_port")
        .add(receive_port, "receive_port,prec", "set receive_port");
  }
};

// class-based, statically typed, event-based API for the state management in CAF
struct coordinator_state {
  std::shared_ptr<NesCoordinator> coordinatorPtr;
  unordered_map<strong_actor_ptr, FogTopologyEntryPtr> actorTopologyMap;
  unordered_map<FogTopologyEntryPtr, strong_actor_ptr> topologyActorMap;

  NodeEngine *enginePtr;
  std::unordered_map<string, tuple<QueryExecutionPlanPtr, OperatorPtr>> runningQueries;
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
    this->state.coordinatorPtr = std::make_shared<NesCoordinator>(NesCoordinator(ip, publish_port, receive_port));
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
  * @brief framework internal method which is called to execute a query or sub-query on a node
  * @param description a description of the query
  * @param executableTransferObject wrapper object with the schema, sources, destinations, operator
  */
  void execute_query(const string &description, string &executableTransferObject);

  /**
   * @brief method which is called to unregister an already running query
   * @param description the description of the query
   */
  void delete_query(const string &description);

  /**
 * @brief send messages to all connected devices and get their operators
 */
  void show_operators();

};

}

#endif //IOTDB_INCLUDE_ACTORS_ACTORCOORDINATOR_HPP_

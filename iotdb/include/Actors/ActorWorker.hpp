//
// Created by xchatziliadis on 26.11.19.
//

#ifndef IOTDB_INCLUDE_ACTORS_ACTORWORKER_HPP_
#define IOTDB_INCLUDE_ACTORS_ACTORWORKER_HPP_

#include <NodeEngine/NodeEngine.hpp>
#include <Operators/Operator.hpp>
#include <caf/all.hpp>
#include <caf/io/all.hpp>

using namespace caf;
using std::string;
using std::tuple;
using std::vector;

namespace iotdb {
constexpr auto task_timeout = std::chrono::seconds(10);

/**
* @brief The configuration file of the worker
*/
class worker_config : public actor_system_config {
 public:
  std::string ip = "127.0.0.1";
  uint16_t receive_port = 0;
  std::string host = "localhost";
  uint16_t publish_port = 4711;
  std::string sensor_type = "cars";

  worker_config() {
    opt_group{custom_options_, "global"}
        .add(ip, "ip", "set ip")
        .add(publish_port, "publish_port,ppub", "set publish_port")
        .add(receive_port, "receive_port,prec", "set receive_port")
        .add(host, "host,H", "set host (ignored in server mode)")
        .add(sensor_type, "sensor_type", "set sensor_type");
  }
};

// the client queues pending tasks
struct worker_state {
  strong_actor_ptr current_server;
  string ip;
  uint16_t publish_port;
  uint16_t receive_port;
  string sensor_type;

  NodeEngine *enginePtr;

  std::unordered_map<string, tuple<QueryExecutionPlanPtr, OperatorPtr>> runningQueries;
};

// class-based, statically typed, event-based API
class actor_worker : public stateful_actor<worker_state> {
 public:
  /**
   * @brief the constructior of the worker to initialize the default objects
   */
  explicit actor_worker(actor_config &cfg, string ip, uint16_t publish_port, uint16_t receive_port, string sensor_type)
      : stateful_actor(
      cfg) {
    this->state.ip = std::move(ip);
    this->state.publish_port = publish_port;
    this->state.receive_port = receive_port;
    this->state.sensor_type = std::move(sensor_type);
    this->state.enginePtr = new NodeEngine();
    this->state.enginePtr->start();
  }

  behavior_type make_behavior() override {
    return init();
  }

 private:
  behavior init();

  behavior unconnected();

  /**
   * @brief the ongoing connection state in the TSM
   * if connection works go to running state, otherwise go to unconnected state
   */
  void connecting(const std::string &host, uint16_t port);

  /**
   * @brief the running of the worker
   */
  behavior running(const actor &coordinator);

  /**
   * @brief framework internal method which is called to execute a query or sub-query on a node
   * @param description a description of the query
   * @param executableTransferObject wrapper object with the schema, sources, destinations, operator
   */
  void execute_query(const string &description, string &executableTransferObject);

  /**
 * @brief method which is called to unregister an already running query
 * @param query the description of the query
 */
  void delete_query(const string &query);

  /**
   * @brief gets the currently locally running operators and returns them as flattened strings in a vector
   * @return the flattend vector<string> object of operators
   */
  vector<string> getOperators();

};

}

#endif //IOTDB_INCLUDE_ACTORS_ACTORWORKER_HPP_

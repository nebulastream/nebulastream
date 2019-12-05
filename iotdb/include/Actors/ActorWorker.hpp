//
// Created by xchatziliadis on 26.11.19.
//

#ifndef IOTDB_INCLUDE_ACTORS_ACTORWORKER_HPP_
#define IOTDB_INCLUDE_ACTORS_ACTORWORKER_HPP_

#include <NodeEngine/NodeEngine.hpp>
#include <Operators/Operator.hpp>
#include <Services/WorkerService.hpp>
#include <caf/all.hpp>
#include <caf/io/all.hpp>
#include <utility>

using namespace caf;
using std::string;
using std::tuple;
using std::vector;

namespace iotdb {

// the client queues pending tasks
struct worker_state {
  strong_actor_ptr current_server;
  std::unique_ptr<WorkerService> workerPtr;
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
    this->state.workerPtr = std::make_unique<WorkerService>(
        WorkerService(std::move(ip), publish_port, receive_port, std::move(sensor_type)));
  }

  behavior_type make_behavior() override {
    return init();
  }

 private:
  behavior init();
  behavior unconnected();
  behavior running(const actor &coordinator);

  /**
   * @brief the ongoing connection state in the TSM
   * if connection works go to running state, otherwise go to unconnected state
   */
  void connecting(const std::string &host, uint16_t port);
};

}

#endif //IOTDB_INCLUDE_ACTORS_ACTORWORKER_HPP_

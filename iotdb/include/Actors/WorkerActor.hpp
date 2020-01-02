#ifndef IOTDB_INCLUDE_ACTORS_WORKERACTOR_HPP_
#define IOTDB_INCLUDE_ACTORS_WORKERACTOR_HPP_

#include <NodeEngine/NodeEngine.hpp>
#include <Operators/Operator.hpp>
#include <Services/WorkerService.hpp>
#include <caf/all.hpp>
#include <caf/io/all.hpp>
#include <utility>
#include <Catalogs/PhysicalStreamConfig.hpp>

using namespace caf;
using std::string;
using std::tuple;
using std::vector;

namespace iotdb {

// the client queues pending tasks
struct WorkerState {
  strong_actor_ptr current_server;
  std::unique_ptr<WorkerService> workerPtr;
};

/**
 * @brief this class handles the actor implementation of the worker
 * class-based, statically typed, event-based API
 * Limitations:
 *  - TODO: this does not handle connection lost
 *  - TODO:
 */
class WorkerActor : public stateful_actor<WorkerState> {
 public:
  /**
   * @brief the constructor to  of the worker to initialize the default objects
   */
  explicit WorkerActor(actor_config &cfg, string ip, uint16_t publish_port,
                       uint16_t receive_port, PhysicalStreamConfig streamConf)
      : stateful_actor(cfg) {
    this->state.workerPtr = std::make_unique<WorkerService>(
        WorkerService(std::move(ip), publish_port, receive_port,
                      std::move(streamConf)));
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

#endif //IOTDB_INCLUDE_ACTORS_WORKERACTOR_HPP_

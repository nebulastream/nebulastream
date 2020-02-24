#ifndef INCLUDE_ACTORS_WORKERACTOR_HPP_
#define INCLUDE_ACTORS_WORKERACTOR_HPP_

#include <NodeEngine/NodeEngine.hpp>
#include <Operators/Operator.hpp>
#include <Services/WorkerService.hpp>
#include <caf/all.hpp>
#include <caf/io/all.hpp>
#include <utility>
#include <caf/blocking_actor.hpp>
#include <Catalogs/PhysicalStreamConfig.hpp>

using namespace caf;
using std::string;
using std::tuple;
using std::vector;

namespace NES {

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
 */
class WorkerActor : public stateful_actor<WorkerState> {

 public:
  /**
   * @brief the constructor to  of the worker to initialize the default objects
   * @param actor config
   * @param ip of this worker
   * @param publish port of this worker
   * @param receive port of thsi worker
   */
  explicit WorkerActor(actor_config &cfg, string ip, uint16_t publish_port,
                       uint16_t receive_port);

  behavior_type make_behavior() override {
    return init();
  }

  /**
   * @brief this methods registers a physical stream via the coordinator to a logical stream
   * @param source type, e.g., CSVSource or OneGenerator source
   * @param config parameter for the source, e.g., file path
   * @param the frequency in which the node should produce data
   * @param the number of buffers to produce
   * @param name of the new physical stream
   * @param name of the logical stream where this physical stream reports to
   * @return bool indicating success
   */
  bool registerPhysicalStream(std::string sourceType, std::string sourceConf,
                              double sourceFrequency,
                              size_t numberOfBuffersToProduce,
                              std::string physicalStreamName,
                              std::string logicalStreamName);

  /**
   * @brief this method registers logical streams via the coordinator
   * @param name of new logical stream
   * @param path to the file containing the schema
   * @return bool indicating the success of the log stream
   * @note the logical stream is not saved in the worker as it is maintained on the coordinator and all logical streams can be retrieved from the physical stream map locally, if we later need the data we can add a map
   */
  bool registerLogicalStream(std::string streamName, std::string filePath);

  /**
   * @brief this method removes the logical stream in the coordinator
   * @param logical stream to be deleted
   * @return bool indicating success of the removal
   */
  bool removeLogicalStream(std::string streamName);

  /**
   * @brief this method removes a physical stream from a logical stream in the coordinator
   * @param logical stream to be deleted
   * @return bool indicating success of the removal
   */
  bool removePhysicalStream(std::string logicalStreamName,
                            std::string physicalStreamName);

 private:
  behavior init();
  behavior unconnected();
  behavior running(const actor &coordinator);
  /**
   * @brief the ongoing connection state in the TSM
   * if connection works go to running state, otherwise go to unconnected state
   */
  bool connecting(const std::string &host, uint16_t port);

  /**
   * @brief this method disconnect the node
   * if connection works go to unconnected state
   */
  bool disconnecting();

};

}

#endif //INCLUDE_ACTORS_WORKERACTOR_HPP_

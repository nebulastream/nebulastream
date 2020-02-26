#ifndef INCLUDE_COMPONENTS_NESWORKER_HPP_
#define INCLUDE_COMPONENTS_NESWORKER_HPP_
#include <Actors/WorkerActor.hpp>
#include <Actors/Configurations/WorkerActorConfig.hpp>
#include <Util/Logger.hpp>
#include <caf/io/all.hpp>
#include <Actors/AtomUtils.hpp>

using namespace std;

namespace NES {

class NesWorker {
 public:
  /**
   * @brief default constructor
   * @note this will create the worker actor using the default worker config
   */
  NesWorker();

  /*
   * @brief start the worker using the default worker config
   * @param bool indicating if the call is blocking
   * @param bool indicating if connect
   * @param port where to publish
   * @return bool indicating success
   */
  bool start(bool blocking, bool withConnect, uint16_t port);

  /*
   * @brief start the worker using the default worker config, and register a new stream
   * @param bool indicating if the call is blocking
   * @param bool indicating if connect
   * @param port where to publish
   * @param new stream of this system
   * @return bool indicating success
   */
  bool startWithRegister(bool blocking, bool withConnect, uint16_t port, PhysicalStreamConfig conf);

  /**
   * @brief stop the worker
   * @return bool indicating success
   */
  bool stop();

  /*
   * @brief connect to coordinator using the default worker config
   * @return bool indicating success
   */
  bool connect();

  /**
   * @brief disconnect from coordinator
   * @return bool indicating success
   */
  bool disconnect();

  /**
   * @brief method to register logical stream with the coordinator
   * @param name of the logical stream
   * @param path tot the file containing the schema
   */
  bool registerLogicalStream(std::string name, std::string path);

  /**
   * @brief method to register physical stream with the coordinator
   * @param config of the stream
   */
  bool registerPhysicalStream(PhysicalStreamConfig conf);

 private:
  bool connected;
  bool withRegisterStream;
  PhysicalStreamConfig conf;
  infer_handle_from_class_t<NES::WorkerActor> workerHandle;
  actor_system *actorSystem;
  WorkerActorConfig workerCfg;
  PhysicalStreamConfig defaultConf;
  std::thread actorThread;
  uint16_t coordinatorPort;
};
typedef std::shared_ptr<NesWorker> NesWorkerPtr;

}
#endif /* INCLUDE_COMPONENTS_NESWORKER_HPP_ */

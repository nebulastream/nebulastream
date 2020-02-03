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

  /**
   * @brief method to connect the worker to the coordinator
   * @param host ip as string
   * @param port as uint
   * @return bool indicating success
   */
  bool connect(std::string host, uint16_t port);

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
   * @param sourceType as string
   * @param sourceConf as string
   * @param pyhsical stream name as string
   * @param logical stream name as string
   */
  bool registerPhysicalStream(std::string sourceType, std::string sourceConf,
                              std::string physicalStreamName,
                              std::string logicalStreamName);

 private:
  bool connected;
  infer_handle_from_class_t<NES::WorkerActor> client;
  WorkerActorConfig workerCfg;
  PhysicalStreamConfig defaultConf;
};
typedef std::shared_ptr<NesWorker> NesWorkerPtr;

}
#endif /* INCLUDE_COMPONENTS_NESWORKER_HPP_ */

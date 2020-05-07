#ifndef INCLUDE_COMPONENTS_NESWORKER_HPP_
#define INCLUDE_COMPONENTS_NESWORKER_HPP_
//#include <Actors/AtomUtils.hpp>

#include <Actors/Configurations/WorkerActorConfig.hpp>
#include <Topology/NESTopologyEntry.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <Catalogs/PhysicalStreamConfig.hpp>
#include <caf/actor_system.hpp>
#include <Actors/WorkerActor.hpp>
//#include <caf/io/all.hpp>

using namespace std;

namespace NES {
class WorkerActor;
typedef std::shared_ptr<WorkerActor> WorkerActorPtr;
class NesWorker {
  public:
    /**
     * @brief default constructor which creates a sensor node
     * @note this will create the worker actor using the default worker config
     */
    NesWorker();

    /**
     * @brief default dtor
     */
    ~NesWorker();

    /**
     * @brief default constructor with option for sensor or worker node
     * @note this will create the worker actor using the default worker config
     */
    NesWorker(NESNodeType type);

    /**
     * @brief start the worker using the default worker config
     * @param bool indicating if the call is blocking
     * @param bool indicating if connect
     * @param port where to publish
     * @param ip of the server
     * @return bool indicating success
     */
    bool start(bool blocking, bool withConnect, uint16_t port,
               std::string serverIp);

    /**
     * @brief configure setup for register a new stream
     * @param new stream of this system
     * @return bool indicating success
     */
    bool setWitRegister(PhysicalStreamConfig conf);

    /**
     * @brief configure setup with set of parent id
     * @param parentId
     * @return bool indicating sucess
     */
    bool setWithParent(std::string parentId);

    /**
     * @brief stop the worker
     * @param force to stop with stopping all queries
     * @return bool indicating success
     */
    bool stop(bool force);

    /**
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
     * @return bool indicating success
     */
    bool registerLogicalStream(std::string name, std::string path);

    /**
     * @brief method to deregister logical stream with the coordinator
     * @param name of the stream
     * @return bool indicating success
     */
    bool deregisterLogicalStream(std::string logicalName);

    /**
     * @brief method to register physical stream with the coordinator
     * @param config of the stream
     * @return bool indicating success
     */
    bool registerPhysicalStream(PhysicalStreamConfig conf);

    /**
    * @brief method to deregister physical stream with the coordinator
    * @param logical and physical of the stream
     * @return bool indicating success
    */
    bool deregisterPhysicalStream(std::string logicalName, std::string physicalName);

    /**
    * @brief method add new parent to this node
    * @param parentId
    * @return bool indicating success
    */
    bool addParent(std::string parentId);

    /**
    * @brief method remove parent from this node
    * @param parentId
    * @return bool indicating success
    */
    bool removeParent(std::string parentId);

    /**
    * @brief method to return the query statistics
    * @param id of the query
    * @return queryStatistics
    */
    QueryStatisticsPtr getQueryStatistics(std::string queryId);

  private:
    size_t getRandomPort(size_t base);

    WorkerActor* actorPtr;
    NodeEnginePtr nodeEngine;

    bool connected;
    bool withRegisterStream;
    PhysicalStreamConfig conf;
    bool withParent;
    std::string parentId;

    infer_handle_from_class_t<NES::WorkerActor> workerHandle;
    actor_system* actorSystem;
    WorkerActorConfig workerCfg;
    uint16_t coordinatorPort;

    NESNodeType type;
    bool stopped;
};
typedef std::shared_ptr<NesWorker> NesWorkerPtr;

}// namespace NES
#endif /* INCLUDE_COMPONENTS_NESWORKER_HPP_ */

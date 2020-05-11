#ifndef INCLUDE_COMPONENTS_NESWORKER_HPP_
#define INCLUDE_COMPONENTS_NESWORKER_HPP_

#include <Actors/Configurations/WorkerActorConfig.hpp>
#include <Topology/NESTopologyEntry.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <Catalogs/PhysicalStreamConfig.hpp>
#include <caf/actor_system.hpp>
#include <Actors/WorkerActor.hpp>
#include <GRPC/CoordinatorRPCClient.hpp>

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
    NesWorker(std::string coordinatorIp,
        std::string coordinatorPort,
        std::string localWorkerIp,
        std::string localWorkerPort,
        NESNodeType type);

    /**
     * @brief default dtor
     */
    ~NesWorker();

    /**
     * @brief start the worker using the default worker config
     * @param bool indicating if the call is blocking
     * @param bool indicating if connect
     * @param port where to publish
     * @param ip of the server
     * @return bool indicating success
     */
    bool start(bool blocking, bool withConnect);

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
    bool unregisterLogicalStream(std::string logicalName);

    /**
     * @brief method to register physical stream with the coordinator
     * @param config of the stream
     * @return bool indicating success
     */
    bool registerPhysicalStream(PhysicalStreamConfig conf);

    /**
    * @brief method to deregister physical stream with the coordinatorf
    * @param logical and physical of the stream
     * @return bool indicating success
    */
    bool unregisterPhysicalStream(std::string logicalName, std::string physicalName);

    /**
    * @brief method add new parent to this node
    * @param parentId
    * @return bool indicating success
    */
    bool addParent(size_t parentId);

    /**
    * @brief method remove parent from this node
    * @param parentId
    * @return bool indicating success
    */
    bool removeParent(size_t parentId);

    /**
    * @brief method to return the query statistics
    * @param id of the query
    * @return queryStatistics
    */
    QueryStatisticsPtr getQueryStatistics(std::string queryId);

    NodeEnginePtr getNodeEngine();
  private:
    size_t getRandomPort(size_t base);

    std::shared_ptr<grpc::Server> rpcServer;
    std::thread rpcThread;


    NodeEnginePtr nodeEngine;
    CoordinatorRPCClientPtr coordinatorRpcClient;

    bool connected;
    bool withRegisterStream;
    PhysicalStreamConfig conf;
    bool withParent;
    std::string parentId;

    std::string coordinatorIp;
    std::string coordinatorPort;
    std::string localWorkerIp;
    std::string localWorkerPort;

//    WorkerActor* workerActor;
//    infer_handle_from_class_t<NES::WorkerActor> workerHandle;
//    actor_system* actorSystem;
//    WorkerActorConfig workerCfg;

    NESNodeType type;
    bool stopped;
};
typedef std::shared_ptr<NesWorker> NesWorkerPtr;

}// namespace NES
#endif /* INCLUDE_COMPONENTS_NESWORKER_HPP_ */

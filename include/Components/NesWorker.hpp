#ifndef INCLUDE_COMPONENTS_NESWORKER_HPP_
#define INCLUDE_COMPONENTS_NESWORKER_HPP_

#include <Catalogs/PhysicalStreamConfig.hpp>
#include <GRPC/CoordinatorRPCClient.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <future>

namespace NES {

class NesWorker {
  public:
    /**
     * @brief default constructor which creates a sensor node
     * @note this will create the worker actor using the default worker config
     */
    NesWorker(std::string coordinatorIp,
              std::string coordinatorPort,
              std::string localWorkerIp,
              uint16_t localWorkerRpcPort,
              uint16_t localWorkerZmqPort,
              uint16_t numberOfSlots,
              NodeType type);

    /**
     * @brief constructor with default numberOfSlots
     */
    NesWorker(std::string coordinatorIp,
              std::string coordinatorPort,
              std::string localWorkerIp,
              uint16_t localWorkerRpcPort,
              uint16_t localWorkerZmqPort,
              NodeType type);

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
    bool setWitRegister(PhysicalStreamConfigPtr conf);

    /**
     * @brief configure setup with set of parent id
     * @param parentId
     * @return bool indicating sucess
     */
    bool setWithParent(std::string parentId);

    /**
     * @brief stop the worker
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
    bool registerPhysicalStream(PhysicalStreamConfigPtr conf);

    /**
    * @brief method to deregister physical stream with the coordinator
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
    * @return vector of queryStatistics
    */
    std::vector<QueryStatisticsPtr> getQueryStatistics(QueryId queryId);

    /**
     * @brief method to get a ptr to the node engine
     * @return pt to node engine
     */
    NodeEnginePtr getNodeEngine();

  private:
    /**
   * @brief this method will start the GRPC Worker server which is responsible for reacting to calls
   */
    void buildAndStartGRPCServer(std::promise<bool>& prom);

    std::shared_ptr<grpc::Server> rpcServer;
    std::shared_ptr<std::thread> rpcThread;

    NodeEnginePtr nodeEngine;
    CoordinatorRPCClientPtr coordinatorRpcClient;

    bool connected;
    bool withRegisterStream;
    PhysicalStreamConfigPtr conf;
    bool withParent;
    std::string parentId;

    std::string rpcAddress;

    std::string coordinatorIp;
    std::string coordinatorPort;
    std::string localWorkerIp;
    uint16_t localWorkerRpcPort;
    uint16_t localWorkerZmqPort;
    uint16_t numberOfSlots;

    NodeType type;
    std::atomic<bool> stopped;

    /**
     * @brief helper method to ensure client is connected before callin rpcs functions
     * @return
     */
    bool waitForConnect();
};
typedef std::shared_ptr<NesWorker> NesWorkerPtr;

}// namespace NES
#endif /* INCLUDE_COMPONENTS_NESWORKER_HPP_ */

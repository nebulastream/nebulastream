#ifndef NES_INCLUDE_GRPC_COORDINATORRPCCLIENT_HPP_
#define NES_INCLUDE_GRPC_COORDINATORRPCCLIENT_HPP_

#include <GRPC/Coordinator.grpc.pb.h>
#include <grpcpp/grpcpp.h>
#include <Catalogs/PhysicalStreamConfig.hpp>
#include <Topology/NESTopologyEntry.hpp>


using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

namespace NES {
class CoordinatorRPCClient {
  public:

    CoordinatorRPCClient(std::shared_ptr<Channel> channel);

    bool connect();

    /**
     * @brief this methods registers a physical stream via the coordinator to a logical stream
     * @param configuration of the stream
     * @return bool indicating success
     */
    bool registerPhysicalStream(PhysicalStreamConfig conf);

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
    bool unregisterLogicalStream(std::string streamName);

    /**
     * @brief this method removes a physical stream from a logical stream in the coordinator
     * @param logical stream to be deleted
     * @return bool indicating success of the removal
     */
    bool unregisterPhysicalStream(std::string logicalStreamName,
                              std::string physicalStreamName);

    /**
     * @brief @brief method to add a new parent to an existing node
     * @param newParentId
     * @return bool indicating success
     */
    bool addParent(size_t parentId);

    /**
     * @brief method to remove a parrent from a node
     * @param newParentId
     * @return bool indicating success
     */
    bool removeParent(size_t parentId);

    /**
     * @brief method to register a node after the connection is established
     * @param type of the node
     * @return bool indicating success
     */
    bool registerNode(NESNodeType type);

    /**
     * @brief method to get own id form server
     * @return own id as listed in the graph
     */
    size_t getId();

    /**
     * @brief method to connect to the coordinator
     * @param host as address of coordinator
     * @param port as the open port on the coordinaotor
     * @return bool indicating success
     */
    bool connecting(const std::string& host, uint16_t port);

    /**
     * @brief this method disconnect the worker from the coordinator
     * @return bool indicating success
     */
    bool disconnecting();

    /**
     * @brief method to shutdown the worker actor
     * @param if force == true all queries will be stopped, if not false will returned in that case
     * @return bool indicating success
     */
    bool shutdown(bool force);

  private:
    std::unique_ptr<CoordinatorService::Stub> stub_;
    size_t workerId;
    std::string ip;
    size_t rpcPort;
    size_t zmqPort;
    size_t numberOfCpus;
    std::string nodeProperties;
    size_t type;

};
}
#endif //




#ifndef NES_PHYSICALNODE_HPP
#define NES_PHYSICALNODE_HPP

#include <NodeStats.pb.h>
#include <Nodes/Node.hpp>

namespace NES {

class PhysicalNode;
typedef std::shared_ptr<PhysicalNode> PhysicalNodePtr;

class PhysicalNode : public Node {

    static PhysicalNodePtr create(uint64_t id, std::string ipAddress, uint32_t grpcPort, uint32_t dataPort, uint16_t resource);

    /**
     * @brief method to get the id of the node
     * @return id as a size_t
     */
    uint64_t getId();

    /**
     * @brief method to get the overall cpu capacity of the node
     * @return size_t cpu capacity
     */
    uint16_t getAvailableResource();

    /**
     * @brief method to reduce the cpu capacity of the node
     * @param size_t of the value that has to be subtracted
     */
    void reduceResource(uint16_t usedCapacity);

    /**
     * @brief method to increase CPU capacity
     * @param size_t of the vlaue that has to be added
     */
    void increaseResource(uint16_t freedCapacity);

    /**
     * @brief method to set the property of the node by creating a NodeProperties object
     * @param a string of the serialized json object of the properties
     */
    void setNodeStats(NodeStats nodeStats);

    /**
   * @brief method to get the node properties
   * @NodePropertiesPtr to the properties of this node
   */
    NodeStats getNodeStats();

    /**
     * @brief method to return this node as a string
     * @param string with the id and the type
     */
    std::string toString();

    /**
     * @brief Get ip address of the node
     * @return ip address
     */
    const std::string& getIpAddress() const;

    /**
     * @brief Get grpc port for the node
     * @return grpc port
     */
    uint32_t getGrpcPort() const;

    /**
     * @brief Get the data port for the node
     * @return data port
     */
    uint32_t getDataPort() const;

  public:
    const std::string toString() const override;

  private:
    explicit PhysicalNode(uint64_t id, std::string ipAddress, uint32_t grpcPort, uint32_t dataPort, uint16_t resource);

    uint64_t id;
    std::string ipAddress;
    uint32_t grpcPort;
    uint32_t dataPort;
    uint16_t resource;
    uint16_t usedResource;
    NodeStats nodeStats;
};
}// namespace NES

#endif//NES_PHYSICALNODE_HPP

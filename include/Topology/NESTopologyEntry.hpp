#ifndef INCLUDE_TOPOLOGY_NESTOPOLOGYENTRY_HPP_
#define INCLUDE_TOPOLOGY_NESTOPOLOGYENTRY_HPP_

#include <NodeStats.pb.h>
#include <Util/CPUCapacity.hpp>
#include <memory>
#include <string>

namespace NES {

class Query;
typedef std::shared_ptr<Query> QueryPtr;

class Pattern;
typedef std::shared_ptr<Pattern> PatternPtr;

enum NESNodeType {
    Coordinator,
    Worker,
    Sensor// TODO what is this?!
};

class NESTopologyEntry {
  public:
    NESTopologyEntry(uint64_t id, std::string ipAddress, int32_t grpcPort, int32_t dataPort, int8_t cpuCapacity);

    /**
     * @brief method to get the id of the node
     * @return id as a size_t
     */
    size_t getId();

    /**
     * @brief method to return the type of this entry
     * @return Coordinator as a parameter
     */
    virtual NESNodeType getEntryType() = 0;

    /**
     * @brief method to return the entry type as a string
     * @return entry type as string
     */
    virtual std::string getEntryTypeString() = 0;

    /**
     * @brief method to get the overall cpu capacity of the node
     * @return size_t cpu capacity
     */
    uint8_t getCpuCapacity();

    /**
     * @brief method to get the actual cpu capacity
     * @param size_t of the current capacity
     */
    uint8_t getRemainingCpuCapacity();

    /**
     * @brief method to reduce the cpu capacity of the node
     * @param size_t of the value that has to be subtracted
     */
    void reduceCpuCapacity(size_t usedCapacity);

    /**
     * @brief method to increase CPU capacity
     * @param size_t of the vlaue that has to be added
     */
    void increaseCpuCapacity(size_t freedCapacity);

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

  protected:
    uint64_t id;
    std::string ipAddress;
    int32_t grpcPort;
    int32_t dataPort;
    int8_t cpuCapacity;
    int8_t remainingCPUCapacity;
    NodeStats nodeStats;
};

typedef std::shared_ptr<NESTopologyEntry> NESTopologyEntryPtr;
}// namespace NES
#endif /* INCLUDE_TOPOLOGY_NESTOPOLOGYENTRY_HPP_ */

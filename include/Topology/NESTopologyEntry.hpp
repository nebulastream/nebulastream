#ifndef INCLUDE_TOPOLOGY_NESTOPOLOGYENTRY_HPP_
#define INCLUDE_TOPOLOGY_NESTOPOLOGYENTRY_HPP_


#include <Util/CPUCapacity.hpp>
#include <string>
#include <memory>

namespace NES {

class Query;
typedef std::shared_ptr<Query> QueryPtr;

class Pattern;
typedef std::shared_ptr<Pattern> PatternPtr;

class NodeStats;
typedef std::shared_ptr<NodeStats> NodeStatsPtr;

enum NESNodeType {
    Coordinator,
    Worker,
    Sensor
};

class NESTopologyEntry {
  public:
    /**
   * @brief method to set the id of a node
   * @param size_t of the id
   */
    void setId(size_t id);

    /**
   * @brief method to get the id of the node
   * @return id as a size_t
   */
    size_t getId();

    /**
   * @brief method to set the ip of a node
   * @param string of the ip
   */
    void setIp(std::string ip);

    /**
   * @brief method to get the id of the node
   * @return ip as a string
   */
    std::string getIp();

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
    virtual size_t getCpuCapacity() = 0;

    /**
   * @brief method to set CPU capacity
   * @param CPUCapacity class describing the node capacity
   */
    virtual void setCpuCapacity(CPUCapacity cpuCapacity) = 0;

    /**
   * @brief method to get the actual cpu capacity
   * @param size_t of the current capacity
   */
    virtual size_t getRemainingCpuCapacity() = 0;

    /**
   * @brief method to reduce the cpu capacity of the node
   * @param size_t of the value that has to be subtracted
   * TODO: this should check if the value becomes less than 0
   */
    virtual void reduceCpuCapacity(size_t usedCapacity) = 0;

    /**
   * @brief method to increase CPU capacity
   * @param size_t of the vlaue that has to be added
   */
    virtual void increaseCpuCapacity(size_t freedCapacity) = 0;

    /**
   * @brief the publish port is the port on which an actor framework server can be accessed
   * @return port to access CAF
   */
    uint16_t getPublishPort();

    /**
   * @brief the publish port is the port on which an actor framework server can be accessed
   * @param port to access CAF
   */
    void setPublishPort(uint16_t publishPort);

    /**
   * @brief the receive port is the port on which internal data transmission via ZMQ is running
   * @return port to access ZMQ
   */
    uint16_t getReceivePort();

    /**
   * @brief the receive port is the port on which internal data transmission via ZMQ is running
   * @param port to access ZMQ
   */
    void setReceivePort(uint16_t receivePort);

    /**
   * @brief the next free receive port on which internal data transmission via ZMQ is running
   * @return receive port as a uint16_t
   * TODO: We need to fix this properly. Currently it just returns the +1 value of the receivePort.
   */
    uint16_t getNextFreeReceivePort();

    /**
   * @brief method to set the property of the node by creating a NodeProperties object
   * @param a string of the serialized json object of the properties
   */
    void setNodeProperty(NodeStatsPtr nodeStats);

    /**
   * @brief method to get the node properties
   * @return serialized json of the node properties object
   */
    std::string getNodeProperty();

    /**
   * @brief method to get the node properties
   * @NodePropertiesPtr to the properties of this node
   */
    NodeStatsPtr getNodeProperties();

    /**
   * @brief method to return this node as a string
   * @param string with the id and the type
   */
    std::string toString();

  protected:
    size_t id;
    std::string ipAddress;
    uint16_t publish_port;
    uint16_t receive_port;
    NodeStatsPtr nodeStats;
};

typedef std::shared_ptr<NESTopologyEntry> NESTopologyEntryPtr;
}// namespace NES
#endif /* INCLUDE_TOPOLOGY_NESTOPOLOGYENTRY_HPP_ */

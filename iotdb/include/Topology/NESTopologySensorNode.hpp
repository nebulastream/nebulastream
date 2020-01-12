#ifndef INCLUDE_TOPOLOGY_NESTOPOLOGYSENSOR_HPP_
#define INCLUDE_TOPOLOGY_NESTOPOLOGYSENSOR_HPP_

#include <memory>
#include <utility>
#include "NESTopologyEntry.hpp"

namespace iotdb {

/**
 * @brief: This class represent a sensor node in nes topology. When you create a sensor node you need to use the setters to
 * define the node id and its cpu capacity.
 *
 * Following are the set of properties that can be defined:
 * 1.) sensor_id : Defines the unique identifier of the node
 * 2.) cpuCapacity : Defines the actual CPU capacity of the node
 * 3.) remainingCPUCapacity : Defined the remaining CPU capacity of the node
 * 4.) query : Defines the query that need to be executed by the node
 */
class NESTopologySensorNode : public NESTopologyEntry {

 public:
  NESTopologySensorNode(size_t nodeId, std::string ip_addr) {
    this->node_id = nodeId;
    this->ip_addr = std::move(ip_addr);
    physicalStreamName = "default_physical";
    cpuCapacity = 0;
    remainingCPUCapacity = 0;
  }

  ~NESTopologySensorNode() = default;

  /**
   * @biref method to set the id of a node
   * @param size_t of the id
   */
  void setId(size_t id);

  /**
   * @brief method to get the id of the node
   * @return id as a size_t
   */
  size_t getId();

  /**
   * @brief method to get the overall cpu capacity of the node
   * @return size_t cpu capacity
   */
  size_t getCpuCapacity();

  /**
   * @brief method to set CPU capacity
   * @param CPUCapacity class describing the node capacity
   */
  void setCpuCapacity(CPUCapacity cpuCapacity);

  /**
   * @brief method to reduce the cpu capacity of the node
   * @param size_t of the value that has to be subtracted
   * TODO: this should check if the value becomes less than 0
   */
  void reduceCpuCapacity(size_t usedCapacity);

  /**
   * @brief method to increase CPU capacity
   * @param size_t of the vlaue that has to be added
   */
  void increaseCpuCapacity(size_t freedCapacity);

  /**
   * @brief method to get the actual cpu capacity
   * @param size_t of the current capacity
   */
  size_t getRemainingCpuCapacity();

  /**
   * @brief method to return the type of this entry
   * @return Coordinator as a parameter
   */
  NESNodeType getEntryType();

  /**
   * @brief method to return the entry type as a string
   * @return entry type as string
   */
  std::string getEntryTypeString();

  /**
   * @brief method to get the overall cpu capacity of the node
   * @return size_t cpu capacity
   * TODO: should this really be here?
   */
  void setQuery(InputQueryPtr pQuery);

  /**
   * @brief method to get the physical stream name of this sensor
   * @return string containing the stream name
   * TODO: add vector mehtod because a node can contain more than one stream
   */
  std::string getPhysicalStreamName();

  /**
   * @brief method to set the physical stream
   * @param string containing the physical stream name
   */
  void setPhysicalStreamName(std::string name);

  /**
   * @brief the publish port is the port on which an actor framework server can be accessed
   * @return port to access CAF
   */
  uint16_t getPublishPort() override;

  /**
   * @brief the publish port is the port on which an actor framework server can be accessed
   * @param port to access CAF
   */
  void setPublishPort(uint16_t publishPort) override;

  /**
   * @brief the receive port is the port on which internal data transmission via ZMQ is running
   * @return port to access ZMQ
   */
  uint16_t getReceivePort() override;

  /**
   * @brief the receive port is the port on which internal data transmission via ZMQ is running
   * @param port to access ZMQ
   */
  void setReceivePort(uint16_t receivePort) override;

  /**
   * @brief the next free receive port on which internal data transmission via ZMQ is running
   * @return receive port as a uint16_t
   * TODO: We need to fix this properly. Currently it just returns the +1 value of the receivePort.
   */
  uint16_t getNextFreeReceivePort() override;

  /**
   * @brief get the ip of this node
   * @return ip as string
   */
  const std::string& getIp() override;

  /**
   * @biref method to set the id of a coordinator node
   * @param size_t of the id
   */
  void setIp(const std::string &ip) override;

 private:
  size_t node_id;
  size_t cpuCapacity;
  size_t remainingCPUCapacity;
  std::string physicalStreamName;
  InputQueryPtr query;
};

typedef std::shared_ptr<NESTopologySensorNode> NESTopologySensorNodePtr;
}  // namespace iotdb
#endif /* INCLUDE_TOPOLOGY_NESTOPOLOGYSENSOR_HPP_ */

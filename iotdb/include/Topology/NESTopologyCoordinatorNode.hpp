#ifndef INCLUDE_TOPOLOGY_NESTOPOLOGYCOORDINATORNODE_HPP_
#define INCLUDE_TOPOLOGY_NESTOPOLOGYCOORDINATORNODE_HPP_

#include <API/InputQuery.hpp>

#include <memory>
#include <vector>
#include <string>
#include "NESTopologyEntry.hpp"

#define INVALID_NODE_ID 101

namespace iotdb {

/**\breif:
 *
 * This class represent a worker node in nes topology.
 * When you create a worker node you need to use the setters to define the node id and its cpu capacity.
 * Following are the set of properties that can be defined:
 * sensor_id : Defines the unique identifier of the node
 * cpuCapacity : Defines the actual CPU capacity of the node
 * remainingCPUCapacity : Defined the remaining CPU capacity of the node
 * isASink : Defines if the node is sink or not. By default a worker node is not a sink node
 * query : Defines the query that need to be executed by the node
 */
class NESTopologyCoordinatorNode : public NESTopologyEntry {

 public:
  /**
   * @brief constructor for coordinator node
   * @param id of the node
   * @param ip of the node
   */
  NESTopologyCoordinatorNode(size_t nodeId, std::string ip_addr);

  ~NESTopologyCoordinatorNode() = default;

  /**
   * @brief method to get the id of a coordinator node
   * @return id as a size_t
   */
  size_t getId() override;

  /**
   * @biref method to set the id of a coordinator node
   * @param size_t of the id
   */
  void setId(size_t id) override;

  /**
   * @brief get the ip of this node
   * @return ip as string
   */
  const std::string& getIp() override;

  /**
   * @brief method to set the ip of the node
   * @param ip as string
   */
  void setIp(const std::string &ip) override;

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
   * @param size_t of the value that has to be substracted
   * TODO: this should check if the value becomes less than 0
   */
  void reduceCpuCapacity(size_t usedCapacity) override;

  /**
   * @brief method to increase CPU capacity
   * @param size_t of the vlaue that has to be added
   */
  void increaseCpuCapacity(size_t freedCapacity) override;

  /**
   * @brief method to get the actual cpu capacity
   * @param size_t of the current capacity
   */
  size_t getRemainingCpuCapacity() override;

  /**
   * @brief method to make this node a sink node
   * @param bool ifSinkNode
   */
  void setSinkNode(bool isASink);

  /**
   * @brief method to get the info if this node is a sink node
   * @return true if this is a sink node otherwise false
   */
  bool getIsASinkNode();

  /**
   * @brief method to return the type of this entry
   * @return Coordinator as a parameter
   */
  NESNodeType getEntryType() override;

  /**
   * @brief method to return the entry type as a string
   * @return if this is a sink then sink else Coordinator"
   */
  std::string getEntryTypeString() override;

  /**
   * @brief method to deploy a query here
   * @param InputQueryPtr to the query for this node
   */
  void setQuery(InputQueryPtr pQuery) override;

  /**
   * @brief method to get the publish port of the node
   * @return publish port
   */
  uint16_t getPublishPort() override;

  /**
   * @brief method to set the publish port of the node
   * @param publish port as a uint16_t
   */
  void setPublishPort(uint16_t publishPort) override;

  /**
   * @brief method to get the receive port of the node
   * @return receive port as a uint16_t
   */
  uint16_t getReceivePort() override;

  /**
   * @brief method to set the receive port of this node
   * @param recieve port as an uint16_t
   */
  void setReceivePort(uint16_t receivePort) override;

  /**
   * @brief method to get the next port (increment by one) based on the current port
   * @return receive port as a uint16_t
   */
  uint16_t getNextFreeReceivePort() override;

 private:
  size_t cpuCapacity;
  size_t remainingCPUCapacity;
  bool isASink;
  InputQueryPtr query;
  size_t node_id;
};

typedef std::shared_ptr<NESTopologyCoordinatorNode> NESTopologyCoordinatorNodePtr;
}  // namespace iotdb
#endif

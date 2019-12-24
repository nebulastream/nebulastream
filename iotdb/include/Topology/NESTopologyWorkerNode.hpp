#ifndef INCLUDE_TOPOLOGY_NESTOPOLOGYWORKERNODE_HPP_
#define INCLUDE_TOPOLOGY_NESTOPOLOGYWORKERNODE_HPP_

#include <API/InputQuery.hpp>

#include <memory>
#include <utility>
#include <vector>
#include <Util/CPUCapacity.hpp>
#include "NESTopologyEntry.hpp"

namespace iotdb {

/**
 * @brief: This class represent a worker node in nes topology. When you create a worker node you need to use the
 * setters to define the node id and its cpu capacity.
*
* Following are the set of properties that can be defined:
* 1.) sensor_id : Defines the unique identifier of the node
* 2.) cpuCapacity : Defines the actual CPU capacity of the node
* 3.) remainingCPUCapacity : Defined the remaining CPU capacity of the node
* 4.) isASink : Defines if the node is sink or not. By default a worker node is not a sink node
* 5.) query : Defines the query that need to be executed by the node
*/
class NESTopologyWorkerNode : public NESTopologyEntry {

 public:
  NESTopologyWorkerNode(size_t nodeId, std::string ip_addr) {
    this->node_id = nodeId;
    this->ip_addr = std::move(ip_addr);
  }
  ~NESTopologyWorkerNode() = default;

  size_t getId() { return node_id; }

  void setId(size_t id) { this->node_id = id; }

  int getCpuCapacity() { return cpuCapacity; }

  void setCpuCapacity(CPUCapacity cpuCapacity) {
    this->cpuCapacity = cpuCapacity.toInt();
    this->remainingCPUCapacity = this->cpuCapacity;
  }

  void reduceCpuCapacity(int usedCapacity) {
    this->remainingCPUCapacity = this->remainingCPUCapacity - usedCapacity;
  }

  void increaseCpuCapacity(int freedCapacity) {
    this->remainingCPUCapacity = this->remainingCPUCapacity + freedCapacity;
  }

  int getRemainingCpuCapacity() { return remainingCPUCapacity; }

  void isASinkNode(bool isASink) {
    this->isASink = isASink;
  }

  bool getIsASinkNode() {
    return this->isASink;
  }

  NESNodeType getEntryType() { return Worker; }

  std::string getEntryTypeString() {
    if (isASink) {
      return "sink";
    }
    return "Worker";
  }

  void setQuery(InputQueryPtr pQuery) { this->query = pQuery; };

  uint16_t getPublishPort() override {
    return publish_port;
  }

  void setPublishPort(uint16_t publishPort) override {
    publish_port = publishPort;
  }

  uint16_t getReceivePort() override {
    return receive_port;
  }

  uint16_t getNextFreeReceivePort() override {
    receive_port++;
    return receive_port;
  }

  void setReceivePort(uint16_t receivePort) override {
    receive_port = receivePort;
  }

  const std::string &getIp() override {
    return this->ip_addr;
  }

  void setIp(const std::string &ip) override {
    this->ip_addr = ip;
  }

 private:
  size_t node_id;
  int cpuCapacity;
  int remainingCPUCapacity;
  bool isASink = false;
  InputQueryPtr query;
};

typedef std::shared_ptr<NESTopologyWorkerNode> NESTopologyWorkerNodePtr;
} // namespace iotdb
#endif /* INCLUDE_TOPOLOGY_NESTOPOLOGYWORKERNODE_HPP_ */

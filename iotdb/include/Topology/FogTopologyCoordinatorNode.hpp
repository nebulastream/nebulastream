#ifndef INCLUDE_TOPOLOGY_FOGTOPOLOGYCOORDINATORNODE_HPP_
#define INCLUDE_TOPOLOGY_FOGTOPOLOGYCOORDINATORNODE_HPP_

#include "FogTopologyEntry.hpp"
#include <API/InputQuery.hpp>

#include <memory>
#include <vector>
#include <string>

#define INVALID_NODE_ID 101

namespace iotdb {

/**\breif:
*
* This class represent a worker node in fog topology.
*
* When you create a worker node you need to use the setters to define the node id and its cpu capacity.
*
* Following are the set of properties that can be defined:
*
* sensor_id : Defines the unique identifier of the node
*
* cpuCapacity : Defines the actual CPU capacity of the node
*
* remainingCPUCapacity : Defined the remaining CPU capacity of the node
*
* isASink : Defines if the node is sink or not. By default a worker node is not a sink node
*
* query : Defines the query that need to be executed by the node
*
*/
class FogTopologyCoordinatorNode : public FogTopologyEntry {

 public:
  FogTopologyCoordinatorNode() { node_id = INVALID_NODE_ID; }

  void setId(size_t id) { this->node_id = id; }

  size_t getId() { return node_id; }

  void setCpuCapacity(int cpuCapacity) {
    this->cpuCapacity = cpuCapacity;
    this->remainingCPUCapacity = cpuCapacity;
  }

  int getCpuCapacity() { return cpuCapacity; }

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

  FogNodeType getEntryType() { return Coordinator; }

  std::string getEntryTypeString() override {
    if (isASink) {
      return "sink";
    }
    return "Coordinator";
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

  void setReceivePort(uint16_t receivePort) override {
    receive_port = receivePort;
  }

  const string &getIp() override {
    return this->ip_addr;
  }

  void setIp(const string &ip) override {
    this->ip_addr = ip;
  }

 private:
  size_t node_id;
  int cpuCapacity;
  int remainingCPUCapacity;
  bool isASink = false;
  InputQueryPtr query;
};

typedef std::shared_ptr<FogTopologyCoordinatorNode> FogTopologyCoordinatorNodePtr;
} // namespace iotdb
#endif /* INCLUDE_TOPOLOGY_FOGTOPOLOGYWORKERNODE_HPP_ */

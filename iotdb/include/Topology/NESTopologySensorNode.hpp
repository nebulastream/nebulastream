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
  }

  ~NESTopologySensorNode() = default;

  void setId(size_t id) override {
    this->node_id = id;
  }

  size_t getId() override {
    return node_id;
  }

  int getCpuCapacity() override {
    return cpuCapacity;
  }

  void setCpuCapacity(CPUCapacity cpuCapacity) {
    this->cpuCapacity = cpuCapacity.toInt();
    this->remainingCPUCapacity = this->cpuCapacity;
  }

  void reduceCpuCapacity(int usedCapacity) override {
    this->remainingCPUCapacity = this->remainingCPUCapacity - usedCapacity;
  }

  void increaseCpuCapacity(int freedCapacity) override {
    this->remainingCPUCapacity = this->remainingCPUCapacity + freedCapacity;
  }

  int getRemainingCpuCapacity() override {
    return remainingCPUCapacity;
  }

  NESNodeType getEntryType() override {
    return Sensor;
  }

  std::string getEntryTypeString() override {
    return "Sensor(" + getSensorType() + ")";
  }

  void setQuery(InputQueryPtr pQuery) override {
    this->query = pQuery;
  }
  ;

  std::string getSensorType() {
    return sensorType;
  }

  void setSensorType(std::string sensorType) {
    this->sensorType = sensorType;
  }

  NodePropertiesPtr getNodeProperties(NodePropertiesPtr nodeProperties) {
    return nodeProperties;
  }

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
  int cpuCapacity { };
  int remainingCPUCapacity { };
  std::string sensorType = "unknown";
  InputQueryPtr query;
};

typedef std::shared_ptr<NESTopologySensorNode> NESTopologySensorNodePtr;
}  // namespace iotdb
#endif /* INCLUDE_TOPOLOGY_NESTOPOLOGYSENSOR_HPP_ */

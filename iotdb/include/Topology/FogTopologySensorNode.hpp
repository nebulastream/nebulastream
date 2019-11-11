#ifndef INCLUDE_TOPOLOGY_FOGTOPOLOGYSENSOR_HPP_
#define INCLUDE_TOPOLOGY_FOGTOPOLOGYSENSOR_HPP_

#include "FogTopologyEntry.hpp"
#include <memory>
#include <Util/CPUCapacity.hpp>

namespace iotdb {

/**
 * @brief: This class represent a sensor node in fog topology. When you create a sensor node you need to use the setters to
 * define the node id and its cpu capacity.
 *
 * Following are the set of properties that can be defined:
 * 1.) sensor_id : Defines the unique identifier of the node
 * 2.) cpuCapacity : Defines the actual CPU capacity of the node
 * 3.) remainingCPUCapacity : Defined the remaining CPU capacity of the node
 * 4.) query : Defines the query that need to be executed by the node
 */
class FogTopologySensorNode : public FogTopologyEntry {

 public:
  FogTopologySensorNode(size_t nodeId, std::string ipAddr) {
    this->nodeId = nodeId;
    this->ipAddr = ipAddr;
  }

  ~FogTopologySensorNode() = default;

  size_t getId() { return nodeId; }

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

  FogNodeType getEntryType() { return Sensor; }

  std::string getEntryTypeString() { return "Sensor"; }

  std::string getIpAddr() override {
    return ipAddr;
  }

  void setQuery(InputQueryPtr pQuery) { this->query = pQuery; };

  std::string getSensorType() {
    return sensorType;
  }

  void setSensorType(std::string sensorType) {
    this->sensorType = sensorType;
  }

 private:
  size_t nodeId;
  int cpuCapacity;
  int remainingCPUCapacity;
  std::string sensorType;
  InputQueryPtr query;
};

typedef std::shared_ptr<FogTopologySensorNode> FogTopologySensorNodePtr;
} // namespace iotdb
#endif /* INCLUDE_TOPOLOGY_FOGTOPOLOGYSENSOR_HPP_ */

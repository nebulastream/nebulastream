#ifndef INCLUDE_TOPOLOGY_FOGTOPOLOGYWORKERNODE_HPP_
#define INCLUDE_TOPOLOGY_FOGTOPOLOGYWORKERNODE_HPP_

#include "FogTopologyEntry.hpp"
#include <API/InputQuery.hpp>

#include <memory>
#include <vector>

#define INVALID_NODE_ID 101

namespace iotdb {

/**
 * @brief: This class represent a worker node in fog topology. When you create a worker node you need to use the
 * setters to define the node id and its cpu capacity.
*
* Following are the set of properties that can be defined:
* 1.) sensor_id : Defines the unique identifier of the node
* 2.) cpuCapacity : Defines the actual CPU capacity of the node
* 3.) remainingCPUCapacity : Defined the remaining CPU capacity of the node
* 4.) isASink : Defines if the node is sink or not. By default a worker node is not a sink node
* 5.) query : Defines the query that need to be executed by the node
*/
class FogTopologyWorkerNode : public FogTopologyEntry {

 public:
  FogTopologyWorkerNode() { node_id = INVALID_NODE_ID; }
  ~FogTopologyWorkerNode() = default;

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

  string getHostname() override {
    return hostName;
  }

  void setHostName(const string &host_name) {
    this->hostName = host_name;
  }

  void isASinkNode(bool isASink) {
    this->isASink = isASink;
  }

  bool getIsASinkNode() {
    return this->isASink;
  }

  FogNodeType getEntryType() { return Worker; }

  std::string getEntryTypeString() {
    if (isASink) {
      return "sink";
    }
    return "Worker";
  }

  void setQuery(InputQueryPtr pQuery) { this->query = pQuery; };

 private:
  size_t node_id;
  int cpuCapacity;
  int remainingCPUCapacity;
  bool isASink = false;
  std::string hostName;
  InputQueryPtr query;
};

typedef std::shared_ptr<FogTopologyWorkerNode> FogTopologyWorkerNodePtr;
} // namespace iotdb
#endif /* INCLUDE_TOPOLOGY_FOGTOPOLOGYWORKERNODE_HPP_ */

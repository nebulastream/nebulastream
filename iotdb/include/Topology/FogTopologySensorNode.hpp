#ifndef INCLUDE_TOPOLOGY_FOGTOPOLOGYSENSOR_HPP_
#define INCLUDE_TOPOLOGY_FOGTOPOLOGYSENSOR_HPP_

#include "FogTopologyEntry.hpp"
#include <memory>

#define INVALID_NODE_ID 101
namespace iotdb {

    /**
     * This class represent a sensor node in fog topology.
     *
     * When you create a sensor node you need to use the setters to define the node id and its cpu capacity.
     *
     */
class FogTopologySensorNode : public FogTopologyEntry {

  public:
    FogTopologySensorNode() { sensor_id = INVALID_NODE_ID; }

    void setId(size_t id) { this->sensor_id = id; }
    size_t getId() { return sensor_id; }
    void setCpuCapacity(int cpuCapacity) { this->cpuCapacity = cpuCapacity; }
    int getCpuCapacity() { return cpuCapacity; }
    void setRemainingCpuCapacity(int usedCapacity) { this->remainingCPUCapacity = this->remainingCPUCapacity- usedCapacity; }
    int getRemainingCpuCapacity() { return remainingCPUCapacity; }

    FogNodeType getEntryType() { return Sensor; }
    std::string getEntryTypeString() { return "Sensor"; }

    void setQuery(InputQueryPtr pQuery) { this->query = pQuery; };

  private:
    size_t sensor_id;
    int cpuCapacity;
    int remainingCPUCapacity;
    InputQueryPtr query;
};

typedef std::shared_ptr<FogTopologySensorNode> FogTopologySensorNodePtr;
} // namespace iotdb
#endif /* INCLUDE_TOPOLOGY_FOGTOPOLOGYSENSOR_HPP_ */

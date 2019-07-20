#ifndef INCLUDE_TOPOLOGY_FOGTOPOLOGYSENSOR_HPP_
#define INCLUDE_TOPOLOGY_FOGTOPOLOGYSENSOR_HPP_

#include "FogTopologyEntry.hpp"
#include <memory>

#define INVALID_NODE_ID 101

namespace iotdb {

    /**\breif:
     * 
     * This class represent a sensor node in fog topology.
     *
     * When you create a sensor node you need to use the setters to define the node id and its cpu capacity.
     *
     * Following are the set of properties that can be defined:
     *
     * sensor_id : Defines the unique identifier of the node
     *
     * cpuCapacity : Defines the actual CPU capacity of the node
     *
     * remainingCPUCapacity : Defined the remaining CPU capacity of the node
     *
     * query : Defines the query that need to be executed by the node
     *
     */
    class FogTopologySensorNode : public FogTopologyEntry {

    public:
        FogTopologySensorNode() { sensor_id = INVALID_NODE_ID; }

        void setId(size_t id) { this->sensor_id = id; }

        size_t getId() { return sensor_id; }

        void setCpuCapacity(int cpuCapacity) { this->cpuCapacity = cpuCapacity; }

        int getCpuCapacity() { return cpuCapacity; }

        void reduceCpuCapacity(int usedCapacity) {
            this->remainingCPUCapacity = this->remainingCPUCapacity - usedCapacity;
        }

        void increaseCpuCapacity(int freedCapacity) {
            this->remainingCPUCapacity = this->remainingCPUCapacity + freedCapacity;
        }

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

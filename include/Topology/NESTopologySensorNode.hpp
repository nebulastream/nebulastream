#ifndef INCLUDE_TOPOLOGY_NESTOPOLOGYSENSOR_HPP_
#define INCLUDE_TOPOLOGY_NESTOPOLOGYSENSOR_HPP_

#include "NESTopologyEntry.hpp"
#include <memory>
#include <utility>

namespace NES {

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
    NESTopologySensorNode(size_t nodeId, std::string ip) {
        this->id = nodeId;
        this->ipAddress = ip;
        physicalStreamName = "default_physical";
        cpuCapacity = 0;
        remainingCPUCapacity = 0;
    }

    ~NESTopologySensorNode() = default;

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
    void setQuery(QueryPtr pQuery);

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

  private:
    size_t cpuCapacity;
    size_t remainingCPUCapacity;
    std::string physicalStreamName;
    QueryPtr query;
};

typedef std::shared_ptr<NESTopologySensorNode> NESTopologySensorNodePtr;
}// namespace NES
#endif /* INCLUDE_TOPOLOGY_NESTOPOLOGYSENSOR_HPP_ */

#ifndef INCLUDE_TOPOLOGY_NESTOPOLOGYCOORDINATORNODE_HPP_
#define INCLUDE_TOPOLOGY_NESTOPOLOGYCOORDINATORNODE_HPP_

#include <API/InputQuery.hpp>

#include "NESTopologyEntry.hpp"
#include <memory>
#include <string>
#include <vector>

#define INVALID_NODE_ID 101

namespace NES {

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

  private:
    size_t cpuCapacity;
    size_t remainingCPUCapacity;
    bool isASink;
    InputQueryPtr query;
};

typedef std::shared_ptr<NESTopologyCoordinatorNode> NESTopologyCoordinatorNodePtr;
}// namespace NES
#endif

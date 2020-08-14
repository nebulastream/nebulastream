#ifndef INCLUDE_TOPOLOGY_NESTOPOLOGYCOORDINATORNODE_HPP_
#define INCLUDE_TOPOLOGY_NESTOPOLOGYCOORDINATORNODE_HPP_

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
     * @param cpuCapacity cpu capacity of the node
     */
    NESTopologyCoordinatorNode(size_t nodeId, std::string ip_addr, int8_t cpuCapacity);

    ~NESTopologyCoordinatorNode() = default;

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

  private:
    bool isASink;
};

typedef std::shared_ptr<NESTopologyCoordinatorNode> NESTopologyCoordinatorNodePtr;
}// namespace NES
#endif

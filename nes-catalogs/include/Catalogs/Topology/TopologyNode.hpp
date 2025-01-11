/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#ifndef NES_CATALOGS_INCLUDE_CATALOGS_TOPOLOGY_TOPOLOGYNODE_HPP_
#define NES_CATALOGS_INCLUDE_CATALOGS_TOPOLOGY_TOPOLOGYNODE_HPP_

#include <Catalogs/Topology/LinkProperty.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongTypeFormat.hpp>
#include <Nodes/Node.hpp>
#include <Util/Mobility/SpatialType.hpp>
#include <Util/TimeMeasurement.hpp>
#include <any>
#include <atomic>
#include <fmt/core.h>
#include <map>
#include <optional>

namespace NES {
class TopologyNode;
using TopologyNodePtr = std::shared_ptr<TopologyNode>;

namespace Spatial::DataTypes::Experimental {
class GeoLocation;
class Waypoint;
}// namespace Spatial::DataTypes::Experimental

namespace Spatial::Mobility::Experimental {
class ReconnectSchedule;
using ReconnectSchedulePtr = std::unique_ptr<ReconnectSchedule>;
}// namespace Spatial::Mobility::Experimental

/**
 * @brief This class represents information about a physical node participating in the NES infrastructure
 */
class TopologyNode : public Node {

  public:
    static TopologyNodePtr create(WorkerId workerId,
                                  const std::string& ipAddress,
                                  uint32_t grpcPort,
                                  uint32_t dataPort,
                                  uint16_t resources,
                                  const std::map<std::string, std::any>& properties);

    explicit TopologyNode(WorkerId workerId,
                          const std::string& ipAddress,
                          uint32_t grpcPort,
                          uint32_t dataPort,
                          uint16_t totalSlots,
                          const std::map<std::string, std::any>& properties);

    ~TopologyNode() override = default;

    /**
     * @brief method to get the id of the node
     * @return id
     */
    WorkerId getId() const;

    /**
     * @brief get grpc address of the topology node
     * @return ip address
     */
    std::string getGrpcAddress() const;

    /**
     * @brief method to get the overall cpu capacity of the node
     * @return uint64_t cpu capacity
     */
    uint16_t getAvailableResources() const;

    /**
     * @brief Get total available resources
     * @return total resources
     */
    uint16_t getTotalResources() const;

    /**
     * @brief Get occupied resources
     * @return occupied resources
     */
    uint16_t getOccupiedResources() const;

    /**
     * @brief method to reduce the cpu capacity of the node
     * @param uint64_t of the value that has to be subtracted
     * @return 
     */
    bool occupySlots(uint16_t occupySlots);

    /**
     * @brief method to increase CPU capacity
     * @param uint64_t of the vlaue that has to be added
     */
    bool releaseSlots(uint16_t freedSlots);

 std::vector<NodePtr> getAllAncestors();
    /**
     * @brief Get ip address of the node
     * @return ip address
     */
    std::string getIpAddress() const;

    /**
     * @brief Get grpc port for the node
     * @return grpc port
     */
    uint32_t getGrpcPort() const;

    /**
     * @brief Get the data port for the node
     * @return data port
     */
    uint32_t getDataPort() const;

    /**
     * @brief Get maintenance flag where 1 represents marked for maintenance
     * @return bool
     */
    bool isUnderMaintenance();

    /**
     * @brief sets maintenance flag where 1 represents marked for maintenance
     * @param flag
     */
    void setForMaintenance(bool flag);

    std::string toString() const override;

    /**
     * @brief Create a shallow copy of the physical node i.e. without copying the parent and child nodes
     * @return
     */
    TopologyNodePtr copy();

    bool containAsParent(NodePtr parentTopologyNode) override;

    bool containAsChild(NodePtr childTopologyNode) override;

    /**
     * @brief Add a new property to the stored properties map
     * @param key key of the new property
     * @param value value of the new property
     */
    void addNodeProperty(const std::string& key, const std::any& value);

    /**
     * @brief Check if a Node property exists
     * @param key key of the property
     * @return true if the property with the given key exists
     */
    bool hasNodeProperty(const std::string& key);

    /**
     * @brief Get a the value of a property
     * @param key key of the value to retrieve
     * @return value of the property with the given key
     */
    std::any getNodeProperty(const std::string& key);

    /**
     * @brief Remove a property from the stored properties map
     * @param key key of the property to remove
     * @return true if the removal is successful
     */
    bool removeNodeProperty(const std::string& key);

    std::vector<WorkerId> getAlternativeNodeCandidateIds();

    void setAlternativeNodeCandidate(WorkerId node);
    /**
     * @brief add a new link property to the stored properties map
     * @param linkedTopologyNodeId topology node to which the property will be associated
     * @param linkProperty the link property
     */
    void addLinkProperty(WorkerId linkedTopologyNodeId, const LinkPropertyPtr& linkProperty);

    /**
     * @brief get a the value of a link property
     * @param linkedTopologyNodeId topology node associated with the link property to retrieve
     * @return value of the link property
     */
    LinkPropertyPtr getLinkProperty(WorkerId linkedTopologyNodeId);

    /**
     * @brief remove a a link property from the stored map
     * @param linkedNodeId topology node associated with the link property to remove
     * @return true if the removal is successful
     */
    bool removeLinkProperty(WorkerId linkedNodeId);

    /**
     * Experimental
     * @brief sets the status of the node as running on a mobile device or a fixed location device.
     * To be run right after node creation. Fixed nodes should not become mobile or vice versa at a later point
     * @param spatialType
     */
    void setSpatialType(NES::Spatial::Experimental::SpatialType spatialType);

    /**
     * Experimental
     * @brief check if the node is a running on a mobile device or not
     * @return true if the node is running on a mobile device
     */
    NES::Spatial::Experimental::SpatialType getSpatialNodeType();

 std::string getFullRPCAddress() const;

 std::vector<NodePtr> getAndFlattenAncestors(std::unordered_set<WorkerId>& visited);

  private:
    WorkerId workerId;
    std::string ipAddress;
    uint32_t grpcPort;
    uint32_t dataPort;
    uint16_t totalSlots;
    uint16_t occupiedSlots;
    std::vector<WorkerId> alternativeNodeIds;

    /**
     * @brief A field to store a map of node properties
     */
    std::map<std::string, std::any> nodeProperties;

    /**
     * @brief A field to store a map of the id of the linked nodes and its link property
     */
    std::map<WorkerId, LinkPropertyPtr> linkProperties;
};
}// namespace NES

namespace fmt {
template<>
struct formatter<NES::TopologyNode> : formatter<std::string> {
    auto format(const NES::TopologyNode& topology_node, format_context& ctx) -> decltype(ctx.out()) {
        return fmt::format_to(ctx.out(),
                              "{} {} {} {} {}",
                              topology_node.getId(),
                              topology_node.getIpAddress(),
                              topology_node.getGrpcPort(),
                              topology_node.getDataPort(),
                              topology_node.getAvailableResources());
    }
};
}//namespace fmt

#endif// NES_CATALOGS_INCLUDE_CATALOGS_TOPOLOGY_TOPOLOGYNODE_HPP_

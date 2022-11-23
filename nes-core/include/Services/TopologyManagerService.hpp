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

#ifndef NES_CORE_INCLUDE_SERVICES_TOPOLOGYMANAGERSERVICE_HPP_
#define NES_CORE_INCLUDE_SERVICES_TOPOLOGYMANAGERSERVICE_HPP_

#include <Spatial/Index/Location.hpp>
#include <Topology/TopologyNode.hpp>
#include <atomic>
#include <memory>
#include <mutex>
#include <vector>
#ifdef S2DEF
#include <s2/base/integral_types.h>
#endif

namespace NES {
class Topology;
using TopologyPtr = std::shared_ptr<Topology>;

class AbstractHealthCheckService;
using HealthCheckServicePtr = std::shared_ptr<AbstractHealthCheckService>;

namespace Spatial::Index::Experimental {
enum class NodeType;
}

/**
 * @brief: This class is responsible for registering/unregistering nodes and adding and removing parentNodes.
 */
class TopologyManagerService {

  public:
    TopologyManagerService(TopologyPtr topology);

    /**
     * @brief registers a node. The node can be a field node (a non mobile worker node with a known location where
     *  mobile devices might connect based on the location info) or a non field node (a non mobile worker node without a known
     *  location which will not be considered as a connect target by mobile devices)
     * @param address of node ip:port
     * @param cpu the cpu capacity of the worker
     * @param nodeProperties of the to be added sensor
     * @param coordinates: an optional containing either the node location in as a Location object if the node is a field node, or nullopt_t for non field nodes
     * @param spatialType: indicates if this worker is running on a fixed location device or on a mobile device or if there is no location data available
     * @return id of node
     */
    uint64_t registerNode(const std::string& address,
                          int64_t grpcPort,
                          int64_t dataPort,
                          uint16_t numberOfSlots,
                          NES::Spatial::Index::Experimental::Location fixedCoordinates,
                          NES::Spatial::Index::Experimental::NodeType spatialType,
                          bool isTfInstalled);

    /**
    * @brief unregister an existing node
    * @param nodeId
    * @return bool indicating success
    */
    bool unregisterNode(uint64_t nodeId);

    /**
    * @brief method to ad a new parent to a node
    * @param childId
    * @param parentId
    * @return bool indicating success
    */
    bool addParent(uint64_t childId, uint64_t parentId);

    /**
     * @brief method to remove an existing parent from a node
     * @param childId
     * @param parentId
     * @return bool indicating success
     */
    bool removeParent(uint64_t childId, uint64_t parentId);

    /**
     * @brief returns a pointer to the node with the specified id
     * @param nodeId
     * @return TopologyNodePtr (or a nullptr if there is no node with this id)
     */
    TopologyNodePtr findNodeWithId(uint64_t nodeId);

    /**
     * Experimental
     * @brief query for topology pointers of the field nodes (non mobile nodes with a known location)
     * within a certain radius around a geographical location
     * @param center: the center of the query area represented as a Location object
     * @param radius: radius in kilometres, all field nodes within this radius around the center will be returned
     * @return vector of pairs containing a pointer to the topology node and the nodes location
     */
    std::vector<std::pair<TopologyNodePtr, NES::Spatial::Index::Experimental::Location>>
    getNodesInRange(NES::Spatial::Index::Experimental::Location center, double radius);

    /**
     * Experimental
     * @brief query for the ids of field nodes within a certain radius around a geographical location
     * @param center: the center of the query area represented as a Location object
     * @param radius: radius in kilometres, all field nodes within this radius around the center will be returned
     * @return vector of pairs containing node ids and the corresponding location
     */
    std::vector<std::pair<uint64_t, NES::Spatial::Index::Experimental::Location>>
    getNodesIdsInRange(NES::Spatial::Index::Experimental::Location center, double radius);

    /**
     * Method to return the root node
     * @return root node
     */
    TopologyNodePtr getRootNode();

    /**
     * @brief This method will remove a given physical node
     * @param nodeToRemove : the node to be removed
     * @return true if successful
     */
    bool removePhysicalNode(const TopologyNodePtr& nodeToRemove);

    /**
     * Sets the health service
     * @param healthCheckService
     */
    void setHealthService(HealthCheckServicePtr healthCheckService);

  private:
    TopologyPtr topology;
    std::mutex registerDeregisterNode;
    std::atomic_uint64_t topologyNodeIdCounter = 0;

    /**
     * @brief method to generate the next (monotonically increasing) topology node id
     * @return next topology node id
     */
    uint64_t getNextTopologyNodeId();

    HealthCheckServicePtr healthCheckService;
};

using TopologyManagerServicePtr = std::shared_ptr<TopologyManagerService>;

}//namespace NES

#endif// NES_CORE_INCLUDE_SERVICES_TOPOLOGYMANAGERSERVICE_HPP_

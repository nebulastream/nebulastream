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

#include <Catalogs/Topology/TopologyNode.hpp>
#include <Identifiers.hpp>
#include <Util/Mobility/GeoLocation.hpp>
#include <atomic>
#include <memory>
#include <mutex>
#include <nlohmann/json.hpp>
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

class LocationIndex;
using LocationIndexPtr = std::shared_ptr<LocationIndex>;
}// namespace Spatial::Index::Experimental

/**
 * @brief: This class is responsible for registering/unregistering nodes and adding and removing parentNodes.
 */
class TopologyManagerService {

  public:
    TopologyManagerService(TopologyPtr topology, NES::Spatial::Index::Experimental::LocationIndexPtr locationIndex);

    /**
     * @brief registers a worker.
     * @param workerId: id of the worker as given in its configuration
     * @param address: address of the worker in ip:port format
     * @param grpcPort: grpc port used by the worker for communication
     * @param dataPort: port used by the worker for receiving or transmitting data
     * @param numberOfSlots: the slots available at the worker
     * @param workerProperties: Additional properties of worker
     * @return unique identifier of the worker
     */
    TopologyNodeId registerWorker(TopologyNodeId workerId,
                                  const std::string& address,
                                  int64_t grpcPort,
                                  int64_t dataPort,
                                  uint16_t numberOfSlots,
                                  std::map<std::string, std::any> workerProperties);

    /**
     * Add GeoLocation of a worker node
     * @param topologyNodeId : worker node id
     * @param geoLocation : location of the worker node
     * @return true if successful
     */
    bool addGeoLocation(TopologyNodeId topologyNodeId, NES::Spatial::DataTypes::Experimental::GeoLocation&& geoLocation);

    /**
     * Update GeoLocation of a worker node
     * @param topologyNodeId : worker node id
     * @param geoLocation : location of the worker node
     * @return true if successful
     */
    bool updateGeoLocation(TopologyNodeId topologyNodeId, NES::Spatial::DataTypes::Experimental::GeoLocation&& geoLocation);

    /**
     * Remove geolocation of worker node
     * @param topologyNodeId : worker id whose location is to be removed
     * @return true if successful
     */
    bool removeGeoLocation(TopologyNodeId topologyNodeId);

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
    TopologyNodePtr findNodeWithId(TopologyNodeId nodeId);

    /**
     * Experimental
     * @brief query for the ids of field nodes within a certain radius around a geographical location
     * @param center: the center of the query area represented as a Location object
     * @param radius: radius in kilometres, all field nodes within this radius around the center will be returned
     * @return vector of pairs containing node ids and the corresponding location
     */
    std::vector<std::pair<TopologyNodeId, NES::Spatial::DataTypes::Experimental::GeoLocation>>
    getNodesIdsInRange(NES::Spatial::DataTypes::Experimental::GeoLocation center, double radius);

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

    /**
     * Get the geo location of the node
     * @param nodeId : node id of the worker
     * @return GeoLocation of the node
     */
    std::optional<NES::Spatial::DataTypes::Experimental::GeoLocation> getGeoLocationForNode(TopologyNodeId nodeId);

    /**
      * @brief function to obtain JSON representation of a NES Topology
      * @param root of the Topology
      * @return JSON representation of the Topology
      */
    nlohmann::json getTopologyAsJson();

  private:
    TopologyPtr topology;
    std::mutex registerDeregisterNode;
    std::atomic_uint64_t topologyNodeIdCounter = 0;
    HealthCheckServicePtr healthCheckService;
    NES::Spatial::Index::Experimental::LocationIndexPtr locationIndex;

    /**
     * @brief method to generate the next (monotonically increasing) topology node id
     * @return next topology node id
     */
    TopologyNodeId getNextTopologyNodeId();
};

using TopologyManagerServicePtr = std::shared_ptr<TopologyManagerService>;

}//namespace NES

#endif// NES_CORE_INCLUDE_SERVICES_TOPOLOGYMANAGERSERVICE_HPP_

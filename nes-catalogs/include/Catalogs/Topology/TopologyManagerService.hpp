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

#ifndef NES_CATALOGS_INCLUDE_CATALOGS_TOPOLOGY_TOPOLOGYMANAGERSERVICE_HPP_
#define NES_CATALOGS_INCLUDE_CATALOGS_TOPOLOGY_TOPOLOGYMANAGERSERVICE_HPP_

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

/**
 * @brief: This class is responsible for registering/unregistering nodes and adding and removing parentNodes.
 */
class TopologyManagerService {

  public:
    TopologyManagerService(const TopologyPtr& topology);

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
    WorkerId registerWorker(WorkerId workerId,
                            const std::string& address,
                            int64_t grpcPort,
                            int64_t dataPort,
                            uint16_t numberOfSlots,
                            std::map<std::string, std::any> workerProperties);

    /**
     * Add GeoLocation of a worker node
     * @param workerId : worker node id
     * @param geoLocation : location of the worker node
     * @return true if successful
     */
    bool addGeoLocation(WorkerId workerId, NES::Spatial::DataTypes::Experimental::GeoLocation&& geoLocation);

    /**
     * Update GeoLocation of a worker node
     * @param workerId : worker node id
     * @param geoLocation : location of the worker node
     * @return true if successful
     */
    bool updateGeoLocation(WorkerId workerId, NES::Spatial::DataTypes::Experimental::GeoLocation&& geoLocation);

    /**
     * Remove geolocation of worker node
     * @param workerId : worker id whose location is to be removed
     * @return true if successful
     */
    bool removeGeoLocation(WorkerId workerId);

    /**
     * @brief unregister an existing node
     * @param workerId
     * @return bool indicating success
     */
    bool unregisterNode(uint64_t workerId);

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
    bool removeAsParent(uint64_t childId, uint64_t parentId);

    /**
     * @brief Add properties to the link between parent and child topology node
     * @param parentWorkerId : the parent topology node id
     * @param childWorkerId : the child topology node id
     * @param bandwidthInMBPS : the link bandwidth in Mega bytes per second
     * @param latencyInMS : the link latency in milliseconds
     * @return true if successful else false
     */
    bool
    addLinkProperty(NES::WorkerId parentWorkerId, NES::WorkerId childWorkerId, uint64_t bandwidthInMBPS, uint64_t latencyInMS);

    /**
     * @brief returns a pointer to the node with the specified id
     * @param nodeId
     * @return TopologyNodePtr (or a nullptr if there is no node with this id)
     */
    TopologyNodePtr findNodeWithId(WorkerId nodeId);

    /**
     * Experimental
     * @brief query for the ids of field nodes within a certain radius around a geographical location
     * @param center: the center of the query area represented as a Location object
     * @param radius: radius in kilometres, all field nodes within this radius around the center will be returned
     * @return vector of pairs containing node ids and the corresponding location
     */
    std::vector<std::pair<WorkerId, NES::Spatial::DataTypes::Experimental::GeoLocation>>
    getTopologyNodeIdsInRange(NES::Spatial::DataTypes::Experimental::GeoLocation center, double radius);

    /**
     * Method to return the root node id
     * @return root node id
     */
    WorkerId getRootTopologyNodeId();

    /**
     * @brief This method will remove a given physical node
     * @param nodeToRemove : the node to be removed
     * @return true if successful
     */
    bool removeTopologyNode(WorkerId nodeToRemove);

    /**
     * Get the geo location of the node
     * @param workerId : node id of the worker
     * @return GeoLocation of the node
     */
    std::optional<NES::Spatial::DataTypes::Experimental::GeoLocation> getGeoLocationForNode(WorkerId workerId);

    /**
      * @brief function to obtain JSON representation of a NES Topology
      * @param root of the Topology
      * @return JSON representation of the Topology
      */
    nlohmann::json getTopologyAsJson();

    /**
     * Get a json containing the id and the location of any node. In case the node is neither a field nor a mobile node,
     * the "location" attribute will be null
     * @param workerId : the id of the requested node
     * @return a json in the format:
        {
            "id": <node id>,
            "location":
                  {
                      "latitude": <latitude>,
                      "longitude": <longitude>
                  }
        }
     */
    nlohmann::json requestNodeLocationDataAsJson(WorkerId workerId);

    /**
     * @brief get a list of all mobile nodes in the system with known locations and their current positions as well as their parent nodes. Mobile nodes without known locations will not appear in the list
     * @return a json list in the format:
     * {
     *      "edges":
     *          [
     *              {
     *                  "source": <node id>,
     *                  "target": <node id>
     *              }
     *          ],
     *      "nodes":
     *          [
     *              {
     *                  "id": <node id>,
     *                  "location":
     *                      {
     *                          "latitude": <latitude>,
     *                          "longitude": <longitude>
     *                      }
     *              }
     *          ]
     *  }
     */
    nlohmann::json requestLocationAndParentDataFromAllMobileNodes();

  private:
    TopologyPtr topology;
    std::atomic_uint64_t topologyNodeIdCounter = 0;

    /**
     * @brief method to generate the next (monotonically increasing) topology node id
     * @return next topology node id
     */
    WorkerId getNextWorkerId();
};

using TopologyManagerServicePtr = std::shared_ptr<TopologyManagerService>;

}//namespace NES

#endif// NES_CATALOGS_INCLUDE_CATALOGS_TOPOLOGY_TOPOLOGYMANAGERSERVICE_HPP_

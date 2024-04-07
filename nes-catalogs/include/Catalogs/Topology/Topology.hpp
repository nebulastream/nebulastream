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

#ifndef NES_CATALOGS_INCLUDE_CATALOGS_TOPOLOGY_TOPOLOGY_HPP_
#define NES_CATALOGS_INCLUDE_CATALOGS_TOPOLOGY_TOPOLOGY_HPP_

#include <Identifiers.hpp>
#include <Util/Mobility/SpatialType.hpp>
#include <any>
#include <folly/Synchronized.h>
#include <map>
#include <memory>
#include <mutex>
#include <nlohmann/json.hpp>
#include <optional>
#include <set>
#include <vector>

namespace NES {

namespace Spatial {

namespace Index::Experimental {
enum class NodeType;

class LocationIndex;
using LocationIndexPtr = std::shared_ptr<LocationIndex>;
}// namespace Index::Experimental

namespace DataTypes::Experimental {
class GeoLocation;
class NetworkCoordinate;
}

namespace Mobility::Experimental {
struct ReconnectPoint;
}

}// namespace Spatial

class TopologyNode;
using TopologyNodePtr = std::shared_ptr<TopologyNode>;

class Topology;
using TopologyPtr = std::shared_ptr<Topology>;

using TopologyNodeWLock = std::shared_ptr<folly::Synchronized<TopologyNodePtr>::WLockedPtr>;

/**
 * @brief This class represents the overall physical infrastructure with different nodes
 */
class Topology {

  public:
    /**
     * @brief Factory to create instance of topology
     * @return shared pointer to the topology
     */
    static TopologyPtr create();

    /**
     * @brief Get IDs of root workers
     * @return vector of root worker ids
     */
    std::vector<WorkerId> getRootWorkerNodeIds();

    /**
     * @brief Set as a topology node as the root node of the topology
     * @param workerId: id of root topology node
     */
    void addAsRootWorkerId(WorkerId workerId);

    /**
     * @brief Register worker as the root of the topology
     * @param newRootWorkerId : the id of the topology node
     * @param address : the host name
     * @param grpcPort : the grpc post
     * @param dataPort : data post
     * @param numberOfSlots : number of slots
     * @param workerProperties : the properties
     * @param bandwidthInMbps: bandwidth in Mbps
     * @param latencyInMs: latency in ms
     * @return worker id
     */
    WorkerId registerWorkerAsRoot(WorkerId newRootWorkerId,
                            const std::string& address,
                            const int64_t grpcPort,
                            const int64_t dataPort,
                            const uint16_t numberOfSlots,
                            std::map<std::string, std::any> workerProperties,
                            uint32_t bandwidthInMbps,
                            uint32_t latencyInMs);

    /**
     * @brief Register a new topology node in the topology
     * @param workerId : the id of the topology node
     * @param address : the host name
     * @param grpcPort : the grpc post
     * @param dataPort : data post
     * @param numberOfSlots : number of slots
     * @param workerProperties : the properties
     * @param bandwidthInMbps: bandwidth in Mbps
     * @param latencyInMs: latency in ms
     * @return worker id
     */
    WorkerId registerWorker(WorkerId workerId,
                                  const std::string& address,
                                  const int64_t grpcPort,
                                  const int64_t dataPort,
                                  const uint16_t numberOfSlots,
                                  std::map<std::string, std::any> workerProperties,
                                  uint32_t bandwidthInMbps,
                                  uint32_t latencyInMs);

    /**
     * @brief returns a vector of parent topology node ids connected to the specified topology node
     * @param nodeId: id of the specified topology node
     * @return vector of parent node ids
     */
    std::vector<WorkerId> getParentTopologyNodeIds(WorkerId nodeId);

    /**
     * @brief returns a vector of child topology node ids connected to the specified topology node
     * @param nodeId: id of the specified topology node
     * @return vector of parent node ids
     */
    std::vector<WorkerId> getChildTopologyNodeIds(WorkerId nodeId);

    /**
     * @brief This method will add the a topology node as child to the parent with provided Id
     * @param parentWorkerId : the id of the parent topology node
     * @param childWorkerId : the new topology node.
     * @return true if successful if one or both node does not exists then false
     */
    bool addTopologyNodeAsChild(WorkerId parentWorkerId, WorkerId childWorkerId);

    /**
     * @brief This method will remove a given topology node
     * @warning If the root node is removed then the next worker that sends a register call will become the root
     * @param topologyNodeId : the node to be removed
     * @return true if successful
     */
    bool unregisterWorker(WorkerId topologyNodeId);

    /**
     * @brief Remove links between two nodes
     * @param parentWorkerId: parent topology node id
     * @param childWorkerId: child topology node id
     * @return true if successful
     */
    bool removeTopologyNodeAsChild(WorkerId parentWorkerId, WorkerId childWorkerId);

    /**
     * @brief Add properties to the link between parent and child topology node
     * @param parentWorkerId : the parent topology node id
     * @param childWorkerId : the child topology node id
     * @param bandwidthInMBPS : the link bandwidth in Mega bytes per second
     * @param latencyInMS : the link latency in milliseconds
     * @return true if successful else false
     */
    bool addLinkProperty(WorkerId parentWorkerId, WorkerId childWorkerId, uint64_t bandwidthInMBPS, uint64_t latencyInMS);

    /**
     * @brief This method will find ad return a copy of the topology node with given worker id
     * @param workerId : the id of the topology node
     * @return Topology node copy if found else nullptr
     */
    TopologyNodePtr getCopyOfTopologyNodeWithId(WorkerId workerId);

    /**
     * @brief checks if a topology node with workerId exists
     * @param workerId: workerId of the node
     * @return true if exists, false otherwise
     */
    bool nodeWithWorkerIdExists(WorkerId workerId);

    /**
     * @brief Mark the node with the id for maintenance
     * @param workerId : worker node id
     * @param state: true or false
     * @return true if successful else false
     */
    bool setForMaintenance(WorkerId workerId, bool state);

    /**
     * @brief acquire the lock on the topology node
     * @param workerId : the id of the topology node
     * @return true if successfully acquired the lock else false
     */
    TopologyNodeWLock lockTopologyNode(WorkerId workerId);

    /**
     * @brief Increase the amount of resources on the node with the id
     * @param workerId : the node id
     * @param amountToRelease : resources to free
     * @return true if successful
     */
    bool releaseSlots(WorkerId workerId, uint16_t amountToRelease);

    /**
     * @brief Reduce the amount of resources on the node with given id
     * @param workerId : the worker id
     * @param amountToOccupy : amount of resources to reduce
     * @return true if successful
     */
    bool occupySlots(WorkerId workerId, uint16_t amountToOccupy);

    /**
     * @brief Get all registered topology nodes
     * @return vector of topology nodes
     */
    std::vector<WorkerId> getAllRegisteredNodeIds();

    /**
     * @brief This method will return a "duplicate" subgraph containing all the paths between start and destination node.
     * Note: this method will only look for the destination nodes that are parent nodes of the source node.
     * @param sourceTopologyNodeId: the source topology node
     * @param destinationTopologyNodeId: the destination topology node
     * @return topology nodes representing the source nodes of the returned subgraph.
     */
    std::optional<TopologyNodePtr> findAllPathBetween(WorkerId sourceTopologyNodeId, WorkerId destinationTopologyNodeId);

    /**
     * @brief Find a sub-graph such that each start node in the given set of start nodes can connect to each destination node in the given set of destination nodes.
     * Note: the returned sub graph is a duplicate of the original sub graph. During the
     * @param sourceTopologyNodeIds: the topology nodes where to start the path identification
     * @param destinationTopologyNodeIds: the topology nodes where to end the path identification
     * @return a vector of start/upstream topology nodes of the sub-graph if all start nodes can connect to all destination nodes else an empty vector
     */
    std::vector<TopologyNodePtr> findPathBetween(const std::vector<WorkerId>& sourceTopologyNodeIds,
                                                 const std::vector<WorkerId>& destinationTopologyNodeIds);

    /**
     * @brief Get topology nodes with the given radius of the geo location
     * @param center : the center geo location
     * @param radius : the radius
     * @return map of topology node id and the geo location
     */
    std::vector<std::pair<WorkerId, NES::Spatial::DataTypes::Experimental::GeoLocation>>
    getTopologyNodeIdsInRange(NES::Spatial::DataTypes::Experimental::GeoLocation center, double radius);

    /**
     * Add GeoLocation of a worker node
     * @param workerId : worker node id
     * @param geoLocation : location of the worker node
     * @return true if successful
     */
    bool addGeoLocation(WorkerId workerId, NES::Spatial::DataTypes::Experimental::GeoLocation&& geoLocation);

    /**
     * Get the geo location of the node
     * @param nodeId : node id of the worker
     * @return GeoLocation of the node
     */
    std::optional<NES::Spatial::DataTypes::Experimental::GeoLocation> getGeoLocationForNode(WorkerId nodeId);

    /**
     * Update GeoLocation of a worker node
     * @param workerId : worker node id
     * @param geoLocation : location of the worker node
     * @return true if successful
     */
    bool updateGeoLocation(WorkerId workerId, NES::Spatial::DataTypes::Experimental::GeoLocation&& geoLocation);

    /**
     * @brief Get spatial type of the topology node with given id
     * @param workerId : the topology node id
     * @return Spatial type if defined else Invalid
     */
    NES::Spatial::Experimental::SpatialType getSpatialType(WorkerId workerId);

    /**
     * @brief: Prepare the topology payload for the elegant placement service
     * @param json representing the payload
     */
    void getElegantPayload(nlohmann::json& payload);

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

    /**
     * @brief Print the current topology information
     */
    void print();

    /**
     * @brief Return graph as JSON
     * @return JSON object representing topology information
     */
    nlohmann::json toJson();

    /**
     * @brief Return graph as string
     * @return string object representing topology information
     */
    std::string toString();

    /**
     * @brief starts a BFS at the node with the supplied id and traverses downstream until either no all downstream roots
     * are reached or all of the supplied target nodes have been found
     * @param startNode the starting point of the BFS
     * @param reachableDownstreamNodes reference to a into which the ids of all visited nodes will be inserted
     * @param targetNodes a list of node ids. Once all nodes in this list have been visited the function will return,
     * even if there are further downstream nodes that have not been visited
     * @return true if all needed locks could be acquired, false if the iteration was aborted because a node could not
     * be locked
     */
    bool findAllDownstreamNodes(const WorkerId& startNode,
                                std::set<WorkerId>& reachableDownstreamNodes,
                                std::vector<WorkerId> targetNodes);

    /**
     * The following part contains the code for network coordinates. Network coordinates are synthetic coordinates in a vector
     * or Euclidean space and used for estimating the network distance between the nodes.
     */

    /**
     * Add NetworkCoordinates of a worker node
     * @param workerId : worker node id
     * @return initial coordinates if successful
     */
    bool addNetworkCoordinate(WorkerId workerId);

    /**
     * Update NetworkCoordinate of a worker node
     * @param workerId : worker node id
     * @param networkCoordinate : synthetic coordinate of the worker node
     * @return true if successful
     */
    bool updateNetworkCoordinate(WorkerId workerId, NES::Spatial::DataTypes::Experimental::NetworkCoordinate&& networkCoordinate);


    /**
     * Get the network coordinate of the node
     * @param nodeId : node id of the worker
     * @return networkCoordinate of the node
     */
    std::optional<NES::Spatial::DataTypes::Experimental::NetworkCoordinate> getNetworkCoordinateForNode(WorkerId nodeId);


    /**
     * Remove networkCoordinate of worker node
     * @param workerId : worker id whose location is to be removed
     * @return true if successful
     */
    bool removeNetworkCoordinate(WorkerId workerId);


  private:
    explicit Topology();

    /**
     * @brief convert a Location to a json representing the same coordinates
     * @param geoLocation : The location object to convert
     * @return a json array in the format:
     * [
     *   <latitude>,
     *   <longitude>,
     * ]
     */
    static nlohmann::json convertLocationToJson(NES::Spatial::DataTypes::Experimental::GeoLocation geoLocation);

    /**
     * Use a node id and a Location to construct a Json representation containing these values.
     * @param workerId : the nodes id
     * @param geoLocation : the nodes location. if this is a nullptr then the "location" attribute of the returned json will be null.
     * @return a json in the format:
        {
            "id": <node id>,
            "location": [
                <latitude>,
                <longitude>
            ]
        }
     */
    static nlohmann::json convertNodeLocationInfoToJson(WorkerId workerId,
                                                        NES::Spatial::DataTypes::Experimental::GeoLocation geoLocation);

    /**
     * @brief convert a NetworkCoordinate to a json representing the same coordinates
     * @param netWorkCoordinate : The location object to convert
     * @return a json array in the format:
     * [
     *   <x1>,
     *   <x2>,
     * ]
     */
    static nlohmann::json convertNetworkCoordinateToJson(NES::Spatial::DataTypes::Experimental::NetworkCoordinate networkCoordinate);

    /**
     * Use a node id and a NetworkCoordinate to construct a Json representation containing these values.
     * @param workerId : the nodes id
     * @param netWorkCoordinate : the nodes coordinate in the selected space. if this is a nullptr then the "coordinate" attribute of the returned json will be null.
     * @return a json in the format:
        {
            "id": <node id>,
            "coordinate": [
                <x1>,
                <x2>
            ]
        }
     */
    static nlohmann::json convertNodeNetworkCoordinateInfoToJson(WorkerId workerId,
                                                        NES::Spatial::DataTypes::Experimental::NetworkCoordinate networkCoordinate);


    /**
     * @brief Merge the sub graphs starting from the nodes into a single sub-graph
     * @param startNodes : start nodes of the sub-graphs to be merged
     * @return start nodes of the merged sub-graph
     */
    static std::vector<TopologyNodePtr> mergeSubGraphs(const std::vector<TopologyNodePtr>& startNodes);

    /**
     * @brief Find if searched nodes are the parent or grand parents of the input test node
     * @param testNode: the test node
     * @param searchedNodes: the searched node
     * @param uniqueNodes: map of all unique worker id to topology nodes observed during the iteration
     * @return the node where the searched node is found
     */
    TopologyNodePtr
    find(TopologyNodePtr testNode, std::vector<TopologyNodePtr> searchedNodes, std::map<WorkerId, TopologyNodePtr>& uniqueNodes);

    /**
     * @brief method to generate the next (monotonically increasing) topology node id
     * @return next topology node id
     */
    WorkerId getNextWorkerId();

    std::vector<WorkerId> rootWorkerIds;
    folly::Synchronized<std::map<WorkerId, folly::Synchronized<TopologyNodePtr>>> workerIdToTopologyNode;
    folly::Synchronized<NES::Spatial::Index::Experimental::LocationIndexPtr> locationIndex;
    static constexpr int BASE_MULTIPLIER = 10000;
    std::atomic_uint64_t topologyNodeIdCounter = 0;
};
}// namespace NES
#endif// NES_CATALOGS_INCLUDE_CATALOGS_TOPOLOGY_TOPOLOGY_HPP_

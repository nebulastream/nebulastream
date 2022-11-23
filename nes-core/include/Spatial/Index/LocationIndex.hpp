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
#ifndef NES_CORE_INCLUDE_SPATIAL_INDEX_LOCATIONINDEX_HPP_
#define NES_CORE_INCLUDE_SPATIAL_INDEX_LOCATIONINDEX_HPP_
#include <Spatial/Index/Location.hpp>
#include <Spatial/Mobility/ReconnectPrediction.hpp>
#include <Util/TimeMeasurement.hpp>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <vector>
#ifdef S2DEF
#include <s2/s2point_index.h>
#endif

namespace NES {

class Topology;
using TopologyPtr = std::shared_ptr<Topology>;

class TopologyNode;
using TopologyNodePtr = std::shared_ptr<TopologyNode>;

namespace Spatial::Index::Experimental {

const int DEFAULT_SEARCH_RADIUS = 50;
class Location;
using LocationPtr = std::shared_ptr<Location>;

/**
 * this class holds information about the geographical position of nodes, for which such a position is known (field nodes)
 * and offers functions to find field nodes within certain ares
 */
class LocationIndex {
  public:
    LocationIndex();

    /**
     * Experimental
     * @brief initialize a field nodes coordinates on creation and add it to the LocatinIndex
     * @param node a pointer to the topology node
     * @param geoLoc  the location of the Field node
     * @return true on success
     */
    bool initializeFieldNodeCoordinates(const TopologyNodePtr& node, Location geoLoc);

    /**
     * Experimental
     * @brief update a field nodes coordinates on will fails if called on non field nodes
     * @param node a pointer to the topology node
     * @param geoLoc  the new location of the Field node
     * @return true on success, false if the node was not a field node
     */
    bool updateFieldNodeCoordinates(const TopologyNodePtr& node, Location geoLoc);

    /**
     * Experimental
     * @brief removes a node from the spatial index. This method is called if a node with a location is unregistered
     * @param node: a pointer to the topology node whose entry is to be removed from the spatial index
     * @returns true on success, false if the node in question does not have a location
     */
    bool removeNodeFromSpatialIndex(const TopologyNodePtr& node);

    /**
     * Experimental
     * @brief returns the closest field node to a certain geographical location
     * @param geoLoc: Coordinates of a location on the map
     * @param radius: the maximum distance which the returned node can have from the specified location
     * @return TopologyNodePtr to the closest field node
     */
    std::optional<TopologyNodePtr> getClosestNodeTo(const Location& geoLoc, int radius = DEFAULT_SEARCH_RADIUS);

    /**
     * Experimental
     * @brief returns the closest field node to a certain node (which does not equal the node passed as an argument)
     * @param nodePtr: pointer to a field node
     * @param radius the maximum distance in kilometres which the returned node can have from the specified node
     * @return TopologyNodePtr to the closest field node unequal to nodePtr
     */
    std::optional<TopologyNodePtr> getClosestNodeTo(const TopologyNodePtr& nodePtr, int radius = DEFAULT_SEARCH_RADIUS);

    /**
     * Experimental
     * @brief get a list of all the nodes within a certain radius around a location
     * @param center: a location around which we look for nodes
     * @param radius: the maximum distance in kilometres of the returned nodes from center
     * @return a vector of pairs containing node pointers and the corresponding locations
     */
    std::vector<std::pair<TopologyNodePtr, Location>> getNodesInRange(Location center, double radius);

    /**
     * Experimental
     * @brief insert a new node into the map keeping track of all the mobile devices in the system
     * @param node: a smart pointer to the node to be inserted
     */
    void addMobileNode(TopologyNodePtr node);

    /**
     * Experimental
     * @brief get the locations of all the nodes in the mobileNodes map
     * @return a vector consisting of pairs containing node id and current location
     */
    std::vector<std::pair<uint64_t, LocationPtr>> getAllMobileNodeLocations();

    /**
     * Experimental
     * @return the amount of field nodes (non mobile nodes with a known location) in the system
     */
    size_t getSizeOfPointIndex();

    /**
     * @brief update the information saved at the coordinator side about a mobile devices predicted next reconnect
     * @param mobileWorkerId : The id of the mobile worker whose predicted reconnect has changed
     * @param reconnectNodeId : The id of the expected new parent after the next reconnect
     * @param reconnectLocation : The location where the mobile device is expected to be at the time of reconnect
     * @param time : The expected time at which the device will reconnect
     * @return true if the information was processed correctly
     */
    bool updatePredictedReconnect(uint64_t mobileWorkerId, Mobility::Experimental::ReconnectPrediction prediction);

    /**
     * @brief get the next scheduled reconnect for a mobile node if there is one saved at the coordinator side
     * @param nodeId : The id of the mobileWorker
     * @return an optional containing the id of the expected new parent, the expected location of the reconnect and the expected time
     * of the reconnect. If no record can be found for this id the return value is nullopt.
     */
    std::optional<Mobility::Experimental::ReconnectPrediction> getScheduledReconnect(uint64_t nodeId);

  private:
    /**
     * Experimental
     * @brief This method sets the location of a field node and adds it to the spatial index. No check for existing entries is
     * performed. To avoid duplicates use initializeFieldNodeCoordinates() or updateFieldNodeCoordinates
     * @param node: a pointer to the topology node
     * @param geoLoc: the (new) location of the field node
     * @return true if successful
     */
    bool setFieldNodeCoordinates(const TopologyNodePtr& node, Location geoLoc);

    std::recursive_mutex locationIndexMutex;

    // a map containing all registered mobile nodes
    std::unordered_map<uint64_t, TopologyNodePtr> mobileNodes;
    std::unordered_map<uint64_t, Mobility::Experimental::ReconnectPrediction> reconnectPredictionMap;
#ifdef S2DEF
    // a spatial index that stores pointers to all the field nodes (non mobile nodes with a known location)
    S2PointIndex<TopologyNodePtr> nodePointIndex;
#endif
};
}//namespace Spatial::Index::Experimental
}//namespace NES
#endif// NES_CORE_INCLUDE_SPATIAL_INDEX_LOCATIONINDEX_HPP_

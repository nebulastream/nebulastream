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
#ifndef NES_CATALOGS_INCLUDE_CATALOGS_TOPOLOGY_INDEX_LOCATIONINDEX_HPP_
#define NES_CATALOGS_INCLUDE_CATALOGS_TOPOLOGY_INDEX_LOCATIONINDEX_HPP_

#include <Identifiers.hpp>
#include <Util/Mobility/GeoLocation.hpp>
#include <Util/TimeMeasurement.hpp>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <vector>
#ifdef S2DEF
#include <s2/s2point_index.h>
#endif

namespace NES::Spatial::Index::Experimental {

const int DEFAULT_SEARCH_RADIUS = 50;

/**
 * this class holds information about the geographical position of nodes, for which such a position is known (field nodes)
 * and offers functions to find field nodes within certain ares
 */
class LocationIndex {
  public:
    LocationIndex();

    /**
     * Experimental
     * @brief initialize a field nodes coordinates on creation and add it to the LocationIndex
     * @param topologyNodeId id of the topology node
     * @param geoLocation  the location of the Field node
     * @return true on success
     */
    bool initializeFieldNodeCoordinates(TopologyNodeId topologyNodeId,
                                        NES::Spatial::DataTypes::Experimental::GeoLocation&& geoLocation);

    /**
     * Experimental
     * @brief update a field nodes coordinates on will fails if called on non field nodes
     * @param topologyNodeId id of the topology node
     * @param geoLocation  the new location of the Field node
     * @return true on success, false if the node was not a field node
     */
    bool updateFieldNodeCoordinates(TopologyNodeId topologyNodeId,
                                    NES::Spatial::DataTypes::Experimental::GeoLocation&& geoLocation);

    /**
     * Experimental
     * @brief removes a node from the spatial index. This method is called if a node with a location is unregistered
     * @param topologyNodeId: id of the topology node whose entry is to be removed from the spatial index
     * @returns true on success, false if the node in question does not have a location
     */
    bool removeNodeFromSpatialIndex(TopologyNodeId topologyNodeId);

    /**
     * Experimental
     * @brief returns the closest field node to a certain geographical location
     * @param geoLocation: Coordinates of a location on the map
     * @param radius: the maximum distance which the returned node can have from the specified location
     * @return TopologyNodePtr to the closest field node
     */
    std::optional<TopologyNodeId> getClosestNodeTo(const NES::Spatial::DataTypes::Experimental::GeoLocation&& geoLocation,
                                                   int radius = DEFAULT_SEARCH_RADIUS) const;

    /**
     * Experimental
     * @brief returns the closest field node to a certain node (which does not equal the node passed as an argument)
     * @param topologyNodeId: topology node id
     * @param radius the maximum distance in kilometres which the returned node can have from the specified node
     * @return TopologyNodePtr to the closest field node unequal to nodePtr
     */
    std::optional<TopologyNodeId> getClosestNodeTo(TopologyNodeId topologyNodeId, int radius = DEFAULT_SEARCH_RADIUS) const;

    /**
     * Experimental
     * @brief get a list of all the nodes within a certain radius around a location
     * @param center: a location around which we look for nodes
     * @param radius: the maximum distance in kilometres of the returned nodes from center
     * @return a vector of pairs containing node pointers and the corresponding locations
     */
    std::vector<std::pair<TopologyNodeId, NES::Spatial::DataTypes::Experimental::GeoLocation>>
    getNodeIdsInRange(const NES::Spatial::DataTypes::Experimental::GeoLocation& center, double radius) const;

    /**
     * Experimental
     * @brief insert a new node into the map keeping track of all the mobile devices in the system
     * @param topologyNodeId: id of the node to be inserted
     * @param geoLocation: location of the node
     */
    void addMobileNode(TopologyNodeId topologyNodeId, NES::Spatial::DataTypes::Experimental::GeoLocation&& geoLocation);

    /**
     * Experimental
     * @brief get the locations of all the nodes with a known location
     * @return a vector consisting of pairs containing node id and current location
     */
    std::vector<std::pair<uint64_t, NES::Spatial::DataTypes::Experimental::GeoLocation>> getAllNodeLocations() const;

    /**
     * Get geolocation of a node
     * @param topologyNodeId : the node id
     * @return geoLocation
     */
    std::optional<NES::Spatial::DataTypes::Experimental::GeoLocation> getGeoLocationForNode(TopologyNodeId topologyNodeId) const;

    /**
     * Experimental
     * @return the amount of field nodes (non mobile nodes with a known location) in the system
     */
    size_t getSizeOfPointIndex();

  private:
    /**
     * Experimental
     * @brief This method sets the location of a field node and adds it to the spatial index. No check for existing entries is
     * performed. To avoid duplicates use initializeFieldNodeCoordinates() or updateFieldNodeCoordinates
     * @param topologyNodeId: id of the topology node
     * @param geoLocation: the (new) location of the field node
     * @return true if successful
     */
    bool setFieldNodeCoordinates(TopologyNodeId topologyNodeId, NES::Spatial::DataTypes::Experimental::GeoLocation&& geoLocation);

    mutable std::recursive_mutex locationIndexMutex;
    // Map containing locations of all registered worker nodes
    std::unordered_map<uint64_t, NES::Spatial::DataTypes::Experimental::GeoLocation> workerGeoLocationMap;
#ifdef S2DEF
    // Spatial index that stores ids of all worker nodes and index them based on their geo location
    S2PointIndex<TopologyNodeId> workerPointIndex;
#endif
};
}// namespace NES::Spatial::Index::Experimental
#endif  // NES_CATALOGS_INCLUDE_CATALOGS_TOPOLOGY_INDEX_LOCATIONINDEX_HPP_

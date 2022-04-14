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
#ifndef NES_GEOLOCATION_GEOSPATIALTOPOLOGY_HPP
#define NES_GEOLOCATION_GEOSPATIALTOPOLOGY_HPP
#include <memory>
#include <optional>
#include <vector>
#ifdef S2DEF
#include <s2/s2point_index.h>
#endif

namespace NES {

class Topology;
using TopologyPtr = std::shared_ptr<Topology>;

class TopologyNode;
using TopologyNodePtr = std::shared_ptr<TopologyNode>;

namespace Experimental::Mobility {

const int DEFAULT_SEARCH_RADIUS = 50;
class GeographicalLocation;

/**
 * this class holds information about the geographical position of nodes, for which such a position is known (field nodes)
 * and offers functions to find field nodes within certain ares
 */
class GeospatialTopology {
  public:

    GeospatialTopology();

    /**
     * Experimental
     * @brief This method sets the location of a new node (making it a field node) or updates the position of an existing field node
     * @param node: a pointer to the topology node
     * @param geoLoc: the (new) location of the field node
     * @param init: defines if the method is called as part of node creation or later on. trying to set a location for a node
     * without existing coordinates will result in failure if the init flag is not set, thus preventing the updating of
     * non field nodes to become field nodes
     * @return true if successful
     */
    bool setFieldNodeCoordinates(const TopologyNodePtr& node, NES::Experimental::Mobility::GeographicalLocation geoLoc, bool init = false);

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
    std::optional<TopologyNodePtr> getClosestNodeTo(const NES::Experimental::Mobility::GeographicalLocation& geoLoc, int radius = NES::Experimental::Mobility::DEFAULT_SEARCH_RADIUS);

    /**
     * Experimental
     * @brief returns the closest field node to a certain node (which does not equal the node passed as an argument)
     * @param nodePtr: pointer to a field node
     * @param radius the maximum distance in kilometres which the returned node can have from the specified node
     * @return TopologyNodePtr to the closest field node unequal to nodePtr
     */
    std::optional<TopologyNodePtr> getClosestNodeTo(const TopologyNodePtr& nodePtr, int radius = NES::Experimental::Mobility::DEFAULT_SEARCH_RADIUS);

    /**
     * Experimental
     * @brief get a list of all the nodes within a certain radius around a location
     * @param center: a location around which we look for nodes
     * @param radius: the maximum distance in kilometres of the returned nodes from center
     * @return a vector of pairs containing node pointers and the corresponding locations
     */
    std::vector<std::pair<TopologyNodePtr, NES::Experimental::Mobility::GeographicalLocation>> getNodesInRange(NES::Experimental::Mobility::GeographicalLocation center, double radius);

    /**
     * Experimental
     * @return the amount of field nodes (non mobile nodes with a known location) in the system
     */
    size_t getSizeOfPointIndex();

  private:
#ifdef S2DEF
    // a spatial index that stores pointers to all the field nodes (non mobile nodes with a known location)
    S2PointIndex<TopologyNodePtr> nodePointIndex;
#endif
};
}//namespace Experimental::Mobility
}//namespace NES
#endif//NES_GEOLOCATION_GEOSPATIALTOPOLOGY_HPP

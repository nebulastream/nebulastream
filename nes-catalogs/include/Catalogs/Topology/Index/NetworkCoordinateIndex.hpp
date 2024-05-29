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
#ifndef NES_CATALOGS_INCLUDE_CATALOGS_TOPOLOGY_INDEX_NETWORKCOORDINATEINDEX_HPP_
#define NES_CATALOGS_INCLUDE_CATALOGS_TOPOLOGY_INDEX_NETWORKCOORDINATEINDEX_HPP_

#include <Identifiers/Identifiers.hpp>
#include <Util/Latency/NetworkCoordinate.hpp>
#include <Util/TimeMeasurement.hpp>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <vector>
#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point.hpp>

namespace NES::Synthetic::Index::Experimental {

const int DEFAULT_SEARCH_RADIUS = 50;

/**
* this class holds information about the network coordinates of nodes, for which such an information is known (field nodes)
* and offers functions to find field nodes within certain area
*/
class NetworkCoordinateIndex {
  public:
    NetworkCoordinateIndex();

    /**
* Experimental
* @brief initialize a field nodes network coordinates on creation and add it to the NetworkCoordinateIndex
* @param topologyNodeId id of the topology node
* @param networkCoordinate  the network coordinate of the Field node
* @return true on success
*/
    bool initializeNetworkCoordinates(WorkerId topologyNodeId,
                                        NES::Synthetic::DataTypes::Experimental::NetworkCoordinate&& networkCoordinate);

    /**
* Experimental
* @brief update a field nodes coordinates on will fails if called on non field nodes
* @param topologyNodeId id of the topology node
* @param networkCoordinate  the new network coordinate of the Field node
* @return true on success, false if the node was not a field node
*/
    bool updateNetworkCoordinates(WorkerId topologyNodeId,
                                    NES::Synthetic::DataTypes::Experimental::NetworkCoordinate&& networkCoordinate);

    /**
* Experimental
* @brief removes a node from the synthetic index. This method is called if a node with a network coordinate is unregistered.
* @param topologyNodeId: id of the topology node whose entry is to be removed from the synthetic index
* @returns true on success, false if the node in question does not have a coordinate
*/
    bool removeNodeFromSyntheticIndex(WorkerId topologyNodeId);

    /**
* Experimental
* @brief returns the closest field node to a certain network coordinate
* @param networkCoordinate: Network coordinates of a node in the synthetic Euclidean space
* @param radius: the maximum distance which the returned node can have from the specified coordinate
* @return TopologyNodePtr to the closest field node
*/
    std::optional<WorkerId> getClosestNodeTo(const NES::Synthetic::DataTypes::Experimental::NetworkCoordinate&& networkCoordinate,
                                             int radius = DEFAULT_SEARCH_RADIUS) const;

    /**
* Experimental
* @brief returns the closest field node to a certain node (which does not equal the node passed as an argument)
* @param topologyNodeId: topology node id
* @param radius the maximum distance in Euclidean distance which the returned node can have from the specified node
* @return TopologyNodePtr to the closest field node unequal to nodePtr
*/
    std::optional<WorkerId> getClosestNodeTo(WorkerId topologyNodeId, int radius = DEFAULT_SEARCH_RADIUS) const;

    /**
* Experimental
* @brief get a list of all the nodes within a certain radius around a network coordinate
* @param center: a location around which we look for nodes
* @param radius: the maximum distance in Euclidean distance of the returned nodes from center
* @return a vector of pairs containing node pointers and the corresponding locations
*/
    std::vector<std::pair<WorkerId, NES::Synthetic::DataTypes::Experimental::NetworkCoordinate>>
    getNodeIdsInRange(const NES::Synthetic::DataTypes::Experimental::NetworkCoordinate& center, double radius) const;

    /**
* Experimental
* @brief insert a new node into the map keeping track of all the devices in the system. The synthetic coordinates of a node
* enables the monitoring of its latency to the other nodes.
* @param topologyNodeId: id of the node to be inserted
* @param networkCoordinate: network coordinate of the node
*/
    void addSyntheticNode(WorkerId topologyNodeId, NES::Synthetic::DataTypes::Experimental::NetworkCoordinate&& networkCoordinate);

    /**
* Experimental
* @brief get the network coordinates of all the nodes with a known coordinate
* @return a vector consisting of pairs containing node id and current coordinate
*/
    std::vector<std::pair<WorkerId , NES::Synthetic::DataTypes::Experimental::NetworkCoordinate>> getAllNodeCoordinates() const;

    /**
* Get networkCoordinate of a node
* @param topologyNodeId : the node id
* @return networkCoordinate
*/
    std::optional<NES::Synthetic::DataTypes::Experimental::NetworkCoordinate> getNetworkCoordinateForNode(WorkerId topologyNodeId) const;

    /**
* Experimental
* @return the amount of synthetic nodes (nodes with known coordinates) in the system
*/
    size_t getSizeOfPointIndex();

  private:
    /**
* Experimental
* @brief This method sets the coordinate of a field node and adds it to the synthetic index. No check for existing entries is
* performed. To avoid duplicates use initializeNetworkCoordinates() or updateNetworkCoordinates()
* @param topologyNodeId: id of the topology node
* @param networkCoordinate: the (new) coordinate of the node
* @return true if successful
*/
    bool setNodeCoordinates(WorkerId topologyNodeId, NES::Synthetic::DataTypes::Experimental::NetworkCoordinate&& networkCoordinate);

    // Map containing network coordinates of all registered worker nodes
    std::unordered_map<WorkerId, NES::Synthetic::DataTypes::Experimental::NetworkCoordinate> workerNetworkCoordinateMap;

    using Point = boost::geometry::model::point<double, 2, boost::geometry::cs::cartesian>;

    using CoordinatePair = std::pair<Point, WorkerId>;

    boost::geometry::index::rtree<CoordinatePair, boost::geometry::index::quadratic<16>> workerPointIndex;

};
}// namespace NES::Synthetic::Index::Experimental
#endif // NES_CATALOGS_INCLUDE_CATALOGS_TOPOLOGY_INDEX_NETWORKCOORDINATEINDEX_HPP_

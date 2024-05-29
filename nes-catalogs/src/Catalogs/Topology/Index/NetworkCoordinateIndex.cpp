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
#include <Util/Latency/Waypoint.hpp>
#include <Util/Latency/NetworkCoordinate.hpp>
#include <Catalogs/Topology/Index/NetworkCoordinateIndex.hpp>
#include <Util/Logger/Logger.hpp>
#include <unordered_map>

#ifdef S2DEF
#include <s2/s2closest_point_query.h>
#include <s2/s2earth.h>
#include <s2/s2latlng.h>
#endif

namespace NES::Synthetic::Index::Experimental {

NetworkCoordinateIndex::NetworkCoordinateIndex() = default;

bool NetworkCoordinateIndex::initializeNetworkCoordinates(WorkerId topologyNodeId,
                                                   NES::Synthetic::DataTypes::Experimental::NetworkCoordinate&& networkCoordinate) {
    return setNodeCoordinates(topologyNodeId, std::move(networkCoordinate));
}

bool NetworkCoordinateIndex::updateNetworkCoordinates(WorkerId topologyNodeId,
                                                      NES::Synthetic::DataTypes::Experimental::NetworkCoordinate&& networkCoordinate) {
    if (removeNodeFromSyntheticIndex(topologyNodeId)) {
        return setNodeCoordinates(topologyNodeId, std::move(networkCoordinate));
    }
    return false;
}

bool NetworkCoordinateIndex::setNodeCoordinates(WorkerId topologyNodeId,
                                                NES::Synthetic::DataTypes::Experimental::NetworkCoordinate&& networkCoordinate) {
    if (!networkCoordinate.isValid()) {
        NES_WARNING("trying to set node's network coordinates to invalid value")
        return false;
    }
    double x1 = networkCoordinate.getX1();
    double x2 = networkCoordinate.getX2();
    NES_DEBUG("updating the network coordinates of Node to: {}, {}", x1, x2);

    Point cartesianCoordinates(Point(x1, x2));
    workerPointIndex.insert(std::make_pair(cartesianCoordinates, topologyNodeId));

    workerNetworkCoordinateMap[topologyNodeId] = networkCoordinate;
    return true;
}

bool NetworkCoordinateIndex::removeNodeFromSyntheticIndex(WorkerId topologyNodeId) {
    auto workerNetworkCoordinate = workerNetworkCoordinateMap.find(topologyNodeId);
    if (workerNetworkCoordinate != workerNetworkCoordinateMap.end()) {
        auto networkCoordinate = workerNetworkCoordinate->second;

        Point point(networkCoordinate.getX1(), networkCoordinate.getX2());
        CoordinatePair pairToRemove = std::make_pair(point, topologyNodeId);

        if (workerPointIndex.remove(pairToRemove) == 1) {
            workerNetworkCoordinateMap.erase(topologyNodeId);
            return true;
        }
        NES_ERROR("Failed to remove worker network coordinate.");
    }
    return false;
}

std::optional<WorkerId> NetworkCoordinateIndex::getClosestNodeTo(const Synthetic::DataTypes::Experimental::NetworkCoordinate&& networkCoordinate,
                                                        int radius) const {
    //Using the squared radius to avoid calculating the square root of the Euclidean distance in the following steps.
    double squaredRadius = radius * radius;

    std::vector<CoordinatePair> closestPoints;

    Point targetCoordinates(networkCoordinate.getX1(), networkCoordinate.getX2());

    workerPointIndex.query(boost::geometry::index::nearest(targetCoordinates, 1), std::back_inserter(closestPoints));

    if (!closestPoints.empty() &&
        boost::geometry::comparable_distance(targetCoordinates, closestPoints.front().first) <= squaredRadius) {
        return closestPoints.front().second;
    }
    NES_DEBUG("There are no workers that are within the {} radius of the target coordinate.", radius);
    return {};
}

std::optional<WorkerId> NetworkCoordinateIndex::getClosestNodeTo(WorkerId topologyNodeId, int radius) const {

    //Using the squared radius to avoid calculating the square root of the Euclidean distance in the following steps.
    double squaredRadius = radius * radius;


    auto workerNetworkCoordinate = workerNetworkCoordinateMap.find(topologyNodeId);
    if (workerNetworkCoordinate == workerNetworkCoordinateMap.end()) {
        NES_ERROR("Node with id {} does not exist.", topologyNodeId);
        return {};
    }

    auto networkCoordinate = workerNetworkCoordinate->second;

    if (!networkCoordinate.isValid()) {
        NES_ERROR("The network coordinates of the node with id {} are invalid!", topologyNodeId);
        return {};
    }

    std::vector<CoordinatePair> closestPoints;

    Point targetCoordinates(networkCoordinate.getX1(), networkCoordinate.getX2());

    workerPointIndex.query(boost::geometry::index::nearest(targetCoordinates, 2), std::back_inserter(closestPoints));

    if (!closestPoints.empty()) {
        if (closestPoints.front().second == topologyNodeId) {
            //The closest worker is the input worker node itself. Switching to the second node.
            if (closestPoints.size() == 2 &&
                boost::geometry::comparable_distance(targetCoordinates, closestPoints[1].first) <= squaredRadius) {
                // Return the topologyNodeId of the second closest node if there is any, since the first one must be the input node itself.
                return closestPoints[1].second;
            }
        } else if (boost::geometry::comparable_distance(targetCoordinates, closestPoints.front().first) <= squaredRadius) {
            // If the first closest point is not the input node, return its id. Test to see what this method does and merge this if clause into 1.
            return closestPoints.front().second;
        }
    }

    NES_DEBUG("There are no nodes in the {} ms range of worker with id {}.", radius, topologyNodeId);
    return {};

}

std::vector<std::pair<WorkerId, Synthetic::DataTypes::Experimental::NetworkCoordinate>>
NetworkCoordinateIndex::getNodeIdsInRange(const Synthetic::DataTypes::Experimental::NetworkCoordinate& center, double radius) const {

    //Using the squared radius to avoid calculating the square root of the Euclidean distance in the following steps.
    double squaredRadius = radius * radius;

    std::vector<CoordinatePair> closestPoints;

    Point targetCoordinates(center.getX1(), center.getX2());

    workerPointIndex.query(boost::geometry::index::satisfies([&](const CoordinatePair& pair) {
                               return boost::geometry::comparable_distance(targetCoordinates, pair.first) <= squaredRadius;
                           }), std::back_inserter(closestPoints));

    std::vector<std::pair<WorkerId, Synthetic::DataTypes::Experimental::NetworkCoordinate>> closestNodeList;

    for (auto pair : closestPoints) {
        closestNodeList.emplace_back(
            pair.second,
            Synthetic::DataTypes::Experimental::NetworkCoordinate(
                boost::geometry::get<0>(pair.first),
                boost::geometry::get<1>(pair.first)));
    }
    return closestNodeList;
}

void NetworkCoordinateIndex::addSyntheticNode(WorkerId topologyNodeId,
                                  NES::Synthetic::DataTypes::Experimental::NetworkCoordinate&& networkCoordinate) {
    workerNetworkCoordinateMap.erase(topologyNodeId);
    workerNetworkCoordinateMap.insert({topologyNodeId, networkCoordinate});
}

std::vector<std::pair<WorkerId , Synthetic::DataTypes::Experimental::NetworkCoordinate>> NetworkCoordinateIndex::getAllNodeCoordinates() const {
    std::vector<std::pair<WorkerId, Synthetic::DataTypes::Experimental::NetworkCoordinate>> coordinateVector;
    coordinateVector.reserve(workerNetworkCoordinateMap.size());
    for (auto& [nodeId, networkCoordinate] : workerNetworkCoordinateMap) {
        if (networkCoordinate.isValid()) {
            coordinateVector.emplace_back(nodeId, networkCoordinate);
        }
    }
    return coordinateVector;
}

size_t NetworkCoordinateIndex::getSizeOfPointIndex() {
    return workerPointIndex.size();
}

std::optional<Synthetic::DataTypes::Experimental::NetworkCoordinate>
NetworkCoordinateIndex::getNetworkCoordinateForNode(WorkerId topologyNodeId) const {
    auto workerNetworkCoordinate = workerNetworkCoordinateMap.find(topologyNodeId);
    if (workerNetworkCoordinate == workerNetworkCoordinateMap.end()) {
        NES_ERROR("Node with id {} does not exist.", topologyNodeId);
        return {};
    }
    return workerNetworkCoordinate->second;
}
}// namespace NES::Synthetic::Index::Experimental

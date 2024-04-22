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
#include <Util/Mobility/Waypoint.hpp>
#include <Util/Mobility/GeoLocation.hpp>
#include <Catalogs/Topology/Index/LocationIndex.hpp>
#include <Util/Logger/Logger.hpp>
#include <unordered_map>
#ifdef S2DEF
#include <s2/s2closest_point_query.h>
#include <s2/s2earth.h>
#include <s2/s2latlng.h>
#endif

namespace NES::Spatial::Index::Experimental {

LocationIndex::LocationIndex() = default;

bool LocationIndex::initializeFieldNodeCoordinates(WorkerId topologyNodeId,
                                                   Spatial::DataTypes::Experimental::GeoLocation&& geoLocation) {
    return setFieldNodeCoordinates(topologyNodeId, std::move(geoLocation));
}

bool LocationIndex::updateFieldNodeCoordinates(WorkerId topologyNodeId,
                                               Spatial::DataTypes::Experimental::GeoLocation&& geoLocation) {
    if (removeNodeFromSpatialIndex(topologyNodeId)) {
        return setFieldNodeCoordinates(topologyNodeId, std::move(geoLocation));
    }
    return false;
}

bool LocationIndex::setFieldNodeCoordinates(WorkerId topologyNodeId,
                                            Spatial::DataTypes::Experimental::GeoLocation&& geoLocation) {
    if (!geoLocation.isValid()) {
        NES_WARNING("trying to set node coordinates to invalid value")
        return false;
    }
    double newLat = geoLocation.getLatitude();
    double newLng = geoLocation.getLongitude();
    NES_DEBUG("updating location of Node to: {}, {}", newLat, newLng);
#ifdef S2DEF
    S2Point newLoc(S2LatLng::FromDegrees(newLat, newLng));
    workerPointIndex.Add(newLoc, topologyNodeId);
#endif
    workerGeoLocationMap[topologyNodeId] = geoLocation;
    return true;
}

bool LocationIndex::removeNodeFromSpatialIndex(WorkerId topologyNodeId) {
    auto workerGeoLocation = workerGeoLocationMap.find(topologyNodeId);
    if (workerGeoLocation != workerGeoLocationMap.end()) {
        auto geoLocation = workerGeoLocation->second;
#ifdef S2DEF
        S2Point point(S2LatLng::FromDegrees(geoLocation.getLatitude(), geoLocation.getLongitude()));
        if (workerPointIndex.Remove(point, topologyNodeId)) {
            workerGeoLocationMap.erase(topologyNodeId);
            return true;
        }
        NES_ERROR("Failed to remove worker location.");
#else
        workerGeoLocationMap.erase(topologyNodeId);
        return true;
#endif
    }
    return false;
}

std::optional<WorkerId> LocationIndex::getClosestNodeTo(const Spatial::DataTypes::Experimental::GeoLocation&& geoLocation,
                                                              int radius) const {
#ifdef S2DEF
    S2ClosestPointQuery<WorkerId> query(&workerPointIndex);
    query.mutable_options()->set_max_distance(S1Angle::Radians(S2Earth::KmToRadians(radius)));
    S2ClosestPointQuery<WorkerId>::PointTarget target(
        S2Point(S2LatLng::FromDegrees(geoLocation.getLatitude(), geoLocation.getLongitude())));
    S2ClosestPointQuery<WorkerId>::Result queryResult = query.FindClosestPoint(&target);
    if (queryResult.is_empty()) {
        return {};
    }
    return queryResult.data();
#else
    NES_WARNING("Files were compiled without s2. Nothing inserted into spatial index");
    (void) geoLocation;
    (void) radius;
    return {};
#endif
}

std::optional<WorkerId> LocationIndex::getClosestNodeTo(WorkerId topologyNodeId, int radius) const {
#ifdef S2DEF
    auto workerGeoLocation = workerGeoLocationMap.find(topologyNodeId);
    if (workerGeoLocation == workerGeoLocationMap.end()) {
        NES_ERROR("Node with id {} does not exists", topologyNodeId);
        return {};
    }

    auto geoLocation = workerGeoLocation->second;
    if (!geoLocation.isValid()) {
        NES_WARNING("Trying to get the closest node to a node that does not have a location")
        return {};
    }

    S2ClosestPointQuery<WorkerId> query(&workerPointIndex);
    query.mutable_options()->set_max_distance(S1Angle::Radians(S2Earth::KmToRadians(radius)));
    S2ClosestPointQuery<WorkerId>::PointTarget target(
        S2Point(S2LatLng::FromDegrees(geoLocation.getLatitude(), geoLocation.getLongitude())));
    auto queryResult = query.FindClosestPoint(&target);
    //if we cannot find any node within the radius return an empty optional
    if (queryResult.is_empty()) {
        return {};
    }
    //if the closest node is different from the input node, return it
    auto closest = queryResult.data();
    if (closest != topologyNodeId) {
        return closest;
    }
    //if the closest node is equal to our input node, we need to look for the second closest
    auto closestPoints = query.FindClosestPoints(&target);
    if (closestPoints.size() < 2) {
        return {};
    }
    return closestPoints[1].data();
#else
    NES_WARNING("Files were compiled without s2, cannot find closest nodes");
    (void) topologyNodeId;
    (void) radius;
    return {};
#endif
}

std::vector<std::pair<WorkerId, Spatial::DataTypes::Experimental::GeoLocation>>
LocationIndex::getNodeIdsInRange(const Spatial::DataTypes::Experimental::GeoLocation& center, double radius) const {
#ifdef S2DEF
    S2ClosestPointQuery<WorkerId> query(&workerPointIndex);
    query.mutable_options()->set_max_distance(S1Angle::Radians(S2Earth::KmToRadians(radius)));

    S2ClosestPointQuery<WorkerId>::PointTarget target(
        S2Point(S2LatLng::FromDegrees(center.getLatitude(), center.getLongitude())));
    auto result = query.FindClosestPoints(&target);
    std::vector<std::pair<WorkerId, Spatial::DataTypes::Experimental::GeoLocation>> closestNodeList;
    for (auto r : result) {
        auto latLng = S2LatLng(r.point());
        closestNodeList.emplace_back(
            r.data(),
            Spatial::DataTypes::Experimental::GeoLocation(latLng.lat().degrees(), latLng.lng().degrees()));
    }
    return closestNodeList;
#else
    NES_WARNING("Files were compiled without s2, cannot find closest nodes");
    (void) center;
    (void) radius;
    return {};
#endif
}

void LocationIndex::addMobileNode(WorkerId topologyNodeId, NES::Spatial::DataTypes::Experimental::GeoLocation&& geoLocation) {
    workerGeoLocationMap.erase(topologyNodeId);
    workerGeoLocationMap.insert({topologyNodeId, geoLocation});
}

std::vector<std::pair<WorkerId, Spatial::DataTypes::Experimental::GeoLocation>> LocationIndex::getAllNodeLocations() const {
    std::vector<std::pair<WorkerId, Spatial::DataTypes::Experimental::GeoLocation>> locationVector;
    locationVector.reserve(workerGeoLocationMap.size());
    for (auto& [nodeId, geoLocation] : workerGeoLocationMap) {
        if (geoLocation.isValid()) {
            locationVector.emplace_back(nodeId, geoLocation);
        }
    }
    return locationVector;
}

size_t LocationIndex::getSizeOfPointIndex() {
#ifdef S2DEF
    return workerPointIndex.num_points();
#else
    NES_WARNING("s2 lib not included");
    return {};
#endif
}

std::optional<Spatial::DataTypes::Experimental::GeoLocation>
LocationIndex::getGeoLocationForNode(WorkerId topologyNodeId) const {
    auto workGeoLocation = workerGeoLocationMap.find(topologyNodeId);
    if (workGeoLocation == workerGeoLocationMap.end()) {
        return {};
    }
    return workGeoLocation->second;
}
}// namespace NES::Spatial::Index::Experimental

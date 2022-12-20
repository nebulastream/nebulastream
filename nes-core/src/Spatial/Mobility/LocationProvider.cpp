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

#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Configurations/Worker/WorkerMobilityConfiguration.hpp>
#include <GRPC/CoordinatorRPCClient.hpp>
#include <Spatial/DataTypes/Waypoint.hpp>
#include <Spatial/Mobility/LocationProvider.hpp>
#include <Spatial/Mobility/LocationProviderCSV.hpp>
#include <Util/Experimental/LocationProviderType.hpp>
#include <Util/Experimental/SpatialType.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Spatial::Mobility::Experimental {

LocationProvider::LocationProvider(Spatial::Experimental::SpatialType spatialType,
                                   DataTypes::Experimental::GeoLocation geoLocation) {
    this->spatialType = spatialType;
    this->workerGeoLocation = geoLocation;
}

Spatial::Experimental::SpatialType LocationProvider::getSpatialType() const { return spatialType; };

DataTypes::Experimental::Waypoint LocationProvider::getWaypoint() {
    switch (spatialType) {
        case Spatial::Experimental::SpatialType::MOBILE_NODE: return getCurrentWaypoint();
        case Spatial::Experimental::SpatialType::FIXED_LOCATION:
            return DataTypes::Experimental::Waypoint(workerGeoLocation);
        case Spatial::Experimental::SpatialType::NO_LOCATION:
        case Spatial::Experimental::SpatialType::INVALID:
            NES_WARNING("Location Provider has invalid spatial type")
            return DataTypes::Experimental::Waypoint(DataTypes::Experimental::Waypoint::invalid());
    }
}

DataTypes::Experimental::NodeIdToGeoLocationMap
LocationProvider::getNodeIdsInRange(const DataTypes::Experimental::GeoLocation& location, double radius) {
    if (!coordinatorRpcClient) {
        NES_WARNING("worker has no coordinator rpc client, cannot download node index");
        return {};
    }
    auto nodeVector = coordinatorRpcClient->getNodeIdsInRange(location, radius);
    return DataTypes::Experimental::NodeIdToGeoLocationMap{nodeVector.begin(), nodeVector.end()};
}

DataTypes::Experimental::NodeIdToGeoLocationMap LocationProvider::getNodeIdsInRange(double radius) {
    auto location = getWaypoint().getLocation();
    if (location.isValid()) {
        return getNodeIdsInRange(std::move(location), radius);
    }
    NES_WARNING("Trying to get the nodes in the range of a node without location");
    return {};
}

void LocationProvider::setCoordinatorRPCClient(CoordinatorRPCClientPtr coordinatorClient) {
    coordinatorRpcClient = coordinatorClient;
}

DataTypes::Experimental::Waypoint LocationProvider::getCurrentWaypoint() {
    //location provider base class will always return invalid current locations
    return DataTypes::Experimental::Waypoint(DataTypes::Experimental::Waypoint::invalid());
}

LocationProviderPtr LocationProvider::create(Configurations::WorkerConfigurationPtr workerConfig) {
    NES::Spatial::Mobility::Experimental::LocationProviderPtr locationProvider;

    switch (workerConfig->mobilityConfiguration.locationProviderType.getValue()) {
        case NES::Spatial::Mobility::Experimental::LocationProviderType::BASE:
            locationProvider =
                std::make_shared<NES::Spatial::Mobility::Experimental::LocationProvider>(workerConfig->nodeSpatialType,
                                                                                         workerConfig->locationCoordinates);
            NES_INFO("creating base location provider")
            break;
        case NES::Spatial::Mobility::Experimental::LocationProviderType::CSV:
            if (workerConfig->mobilityConfiguration.locationProviderConfig.getValue().empty()) {
                NES_FATAL_ERROR("cannot create csv location provider if no provider config is set")
                exit(EXIT_FAILURE);
            }
            locationProvider = std::make_shared<NES::Spatial::Mobility::Experimental::LocationProviderCSV>(
                workerConfig->mobilityConfiguration.locationProviderConfig,
                workerConfig->mobilityConfiguration.locationProviderSimulatedStartTime);
            break;
        case NES::Spatial::Mobility::Experimental::LocationProviderType::INVALID:
            NES_FATAL_ERROR("Trying to create location provider but provider type is invalid")
            exit(EXIT_FAILURE);
    }

    return locationProvider;
}
}// namespace NES::Spatial::Mobility::Experimental

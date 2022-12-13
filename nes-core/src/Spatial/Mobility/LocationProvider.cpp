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
#include <Spatial/Index/Waypoint.hpp>
#include <Spatial/Mobility/LocationProvider.hpp>
#include <Spatial/Mobility/LocationProviderCSV.hpp>
#include <Util/Experimental/LocationProviderType.hpp>
#include <Util/Experimental/NodeType.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Spatial::Mobility::Experimental {

LocationProvider::LocationProvider(Index::Experimental::NodeType spatialType, Index::Experimental::Location fieldNodeLoc) {
    this->nodeType = spatialType;
    this->fixedLocationCoordinates = std::make_shared<Index::Experimental::Location>(fieldNodeLoc);
}

Index::Experimental::NodeType LocationProvider::getNodeType() const { return nodeType; };

bool LocationProvider::setFixedLocationCoordinates(const Index::Experimental::Location& geoLoc) {
    if (nodeType != Index::Experimental::NodeType::FIXED_LOCATION) {
        return false;
    }
    fixedLocationCoordinates = std::make_shared<Index::Experimental::Location>(geoLoc);
    return true;
}

Index::Experimental::WaypointPtr LocationProvider::getWaypoint() {
    switch (nodeType) {
        case Index::Experimental::NodeType::MOBILE_NODE: return getCurrentWaypoint();
        case Index::Experimental::NodeType::FIXED_LOCATION:
            return std::make_shared<Index::Experimental::Waypoint>(*fixedLocationCoordinates);
        case Index::Experimental::NodeType::NO_LOCATION: return {};
        case Index::Experimental::NodeType::INVALID:
            NES_WARNING("Location Provider has invalid spatial type")
            return std::make_shared<Index::Experimental::Waypoint>(Index::Experimental::Waypoint::invalid());
    }
}

Index::Experimental::NodeIdsMapPtr LocationProvider::getNodeIdsInRange(Index::Experimental::LocationPtr location, double radius) {
    if (!coordinatorRpcClient) {
        NES_WARNING("worker has no coordinator rpc client, cannot download node index");
        return {};
    }
    auto nodeVector = coordinatorRpcClient->getNodeIdsInRange(location, radius);
    return std::make_shared<std::unordered_map<uint64_t, Index::Experimental::Location>>(nodeVector.begin(), nodeVector.end());
}

Index::Experimental::NodeIdsMapPtr LocationProvider::getNodeIdsInRange(double radius) {
    auto location = getWaypoint()->getLocation();
    if (location && location->isValid()) {
        return getNodeIdsInRange(location, radius);
    }
    NES_WARNING("Trying to get the nodes in the range of a node without location");
    return {};
}

void LocationProvider::setCoordinatorRPCCLient(CoordinatorRPCClientPtr coordinatorClient) {
    coordinatorRpcClient = coordinatorClient;
}

Index::Experimental::WaypointPtr LocationProvider::getCurrentWaypoint() {
    //location provider base class will always return invalid current locations
    return std::make_shared<Index::Experimental::Waypoint>(Index::Experimental::Waypoint::invalid());
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

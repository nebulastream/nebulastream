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
#include <Geolocation/LocationService.hpp>
#include <Geolocation/LocationProviderCSV.hpp>
#include <Util/Logger/Logger.hpp>
#include <GRPC/CoordinatorRPCClient.hpp>

namespace NES::Experimental::Mobility {

LocationService::LocationService(bool isMobile, Location fieldNodeLoc ) {
    this->isMobile = isMobile;
    this->fixedLocationCoordinates = std::make_shared<Location>(fieldNodeLoc);
}

bool LocationService::createLocationProvider(LocationProviderType type, std::string config) {
    if (config.empty()) {
        NES_FATAL_ERROR("isMobile flag is set, but there is no proper configuration for the location source. exiting");
        exit(EXIT_FAILURE);
    }

    switch (type) {
        case LocationProviderType::NONE:
            NES_FATAL_ERROR("Trying to create location provider but provider type is set to NONE")
            exit(EXIT_FAILURE);
        case LocationProviderType::CSV: locationProvider = std::make_shared<LocationProviderCSV>(config);
            break;
        case LocationProviderType::INVALID:
            NES_FATAL_ERROR("Trying to create location provider but provider type is invalid")
            exit(EXIT_FAILURE);
    }

    return true;
}

void LocationService::setCoordinatorRPCClient(CoordinatorRPCClientPtr rpcClientPtr) {
    this->coordinatorRpcClient = rpcClientPtr;
}

bool LocationService::isFieldNode() { return fixedLocationCoordinates->isValid() && !isMobile; }

bool LocationService::isMobileNode() const { return isMobile; };

bool LocationService::setFixedLocationCoordinates(const Location& geoLoc) {
    if (isMobile) {
        return false;
    }
    fixedLocationCoordinates = std::make_shared<Location>(geoLoc);
    return true;
}

Location LocationService::getLocation() {
    if (isMobile) {
        if (locationProvider) {
            return locationProvider->getCurrentLocation().first;
        }
        //if the node is mobile, but there is no location Source, return invalid
        NES_WARNING("Node is mobile but does not have a location source");
        return {};
    }
    return *fixedLocationCoordinates;
}

std::vector<std::pair<uint64_t, Location>>
LocationService::getNodeIdsInRange(Location coord, double radius) {
    return coordinatorRpcClient->getNodeIdsInRange(coord, radius);
}

std::vector<std::pair<uint64_t, Location>> LocationService::getNodeIdsInRange(double radius) {
    auto coord = getLocation();
    if (coord.isValid()) {
        return getNodeIdsInRange(coord, radius);
    }
    NES_WARNING("Trying to get the nodes in the range of a node without location");
    return {};
}
}// namespace NES::Experimental::Mobility

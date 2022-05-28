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
#include <GRPC/CoordinatorRPCClient.hpp>
#include <Spatial/LocationProviderCSV.hpp>
#include <Spatial/NodeLocationWrapper.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Spatial::Mobility::Experimental {

NodeLocationWrapper::NodeLocationWrapper(bool isMobile, Index::Experimental::Location fieldNodeLoc) {
    this->isMobile = isMobile;
    this->fixedLocationCoordinates = std::make_shared<Index::Experimental::Location>(fieldNodeLoc);
}

bool NodeLocationWrapper::createLocationProvider(LocationProviderType type, std::string config) {
    if (config.empty()) {
        NES_FATAL_ERROR("isMobile flag is set, but there is no proper configuration for the location source. exiting");
        exit(EXIT_FAILURE);
    }

    switch (type) {
        case LocationProviderType::NONE:
            NES_FATAL_ERROR("Trying to create location provider but provider type is set to NONE")
            exit(EXIT_FAILURE);
        case LocationProviderType::CSV: locationProvider = std::make_shared<LocationProviderCSV>(config); break;
        case LocationProviderType::INVALID:
            NES_FATAL_ERROR("Trying to create location provider but provider type is invalid")
            exit(EXIT_FAILURE);
    }

    return true;
}

void NodeLocationWrapper::setCoordinatorRPCClient(CoordinatorRPCClientPtr rpcClientPtr) {
    this->coordinatorRpcClient = rpcClientPtr;
}

bool NodeLocationWrapper::isFieldNode() { return fixedLocationCoordinates->isValid() && !isMobile; }

bool NodeLocationWrapper::isMobileNode() const { return isMobile; };

bool NodeLocationWrapper::setFixedLocationCoordinates(const Index::Experimental::Location& geoLoc) {
    if (isMobile) {
        return false;
    }
    fixedLocationCoordinates = std::make_shared<Index::Experimental::Location>(geoLoc);
    return true;
}

Index::Experimental::LocationPtr NodeLocationWrapper::getLocation() {
    if (isMobile) {
        if (locationProvider) {
            NES_DEBUG("Node location wrapper returning mobile node location: "
                      << locationProvider->getCurrentLocation().first->getLatitude() << ", "
                      << locationProvider->getCurrentLocation().first->getLongitude())
            return locationProvider->getCurrentLocation().first;
        }
        //if the node is mobile, but there is no location Source, return invalid
        NES_WARNING("Node is mobile but does not have a location source");
        return std::make_shared<Spatial::Index::Experimental::Location>();
    }
    return fixedLocationCoordinates;
}

std::vector<std::pair<uint64_t, Index::Experimental::Location>>
NodeLocationWrapper::getNodeIdsInRange(Index::Experimental::Location coord, double radius) {
    return coordinatorRpcClient->getNodeIdsInRange(coord, radius);
}

std::vector<std::pair<uint64_t, Index::Experimental::Location>> NodeLocationWrapper::getNodeIdsInRange(double radius) {
    auto coord = getLocation();
    if (coord->isValid()) {
        return getNodeIdsInRange(*coord, radius);
    }
    NES_WARNING("Trying to get the nodes in the range of a node without location");
    return {};
}

LocationProviderPtr NodeLocationWrapper::getLocationProvider() { return locationProvider; }
}// namespace NES::Spatial::Mobility::Experimental

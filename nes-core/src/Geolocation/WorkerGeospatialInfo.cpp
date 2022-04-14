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
#include "Geolocation/WorkerGeospatialInfo.hpp"
#include "Util/Logger/Logger.hpp"
#include "Geolocation/LocationSourceCSV.hpp"
#include <GRPC/CoordinatorRPCClient.hpp>

namespace NES::Experimental::Mobility {

WorkerGeospatialInfo::WorkerGeospatialInfo(bool isMobile, GeographicalLocation fieldNodeLoc ) {
    this->isMobile = isMobile;
    this->fixedLocationCoordinates = std::make_shared<NES::Experimental::Mobility::GeographicalLocation>(fieldNodeLoc);
}

bool WorkerGeospatialInfo::createLocationSource(NES::Experimental::Mobility::LocationSource::Type type, std::string config) {
    if (config.empty()) {
        NES_FATAL_ERROR("isMobile flag is set, but there is no proper configuration for the location source. exiting");
        exit(EXIT_FAILURE);
    }

    switch (type) {
        case NES::Experimental::Mobility::LocationSource::csv:
            locationSource = std::make_shared<NES::Experimental::Mobility::LocationSourceCSV>(config);
            break;
    }

    return true;
}

void WorkerGeospatialInfo::setRPCClient(CoordinatorRPCClientPtr rpcClientPtr) {
    this->coordinatorRpcClient = rpcClientPtr;
}

bool WorkerGeospatialInfo::isFieldNode() { return fixedLocationCoordinates->isValid() && !isMobile; }

bool WorkerGeospatialInfo::isMobileNode() const { return isMobile; };

bool WorkerGeospatialInfo::setFixedLocationCoordinates(const NES::Experimental::Mobility::GeographicalLocation& geoLoc) {
    if (isMobile) {
        return false;
    }
    fixedLocationCoordinates = std::make_shared<NES::Experimental::Mobility::GeographicalLocation>(geoLoc);
    return true;
}

NES::Experimental::Mobility::GeographicalLocation WorkerGeospatialInfo::getGeoLoc() {
    if (isMobile) {
        if (locationSource) {
            return locationSource->getCurrentLocation().first;
        }
        //if the node is mobile, but there is no location Source, return invalid
        NES_WARNING("Node is mobile but does not have a location source");
        return {};
    }
    return *fixedLocationCoordinates;
}

std::vector<std::pair<uint64_t, NES::Experimental::Mobility::GeographicalLocation>> WorkerGeospatialInfo::getNodeIdsInRange(NES::Experimental::Mobility::GeographicalLocation coord, double radius) {
    return coordinatorRpcClient->getNodeIdsInRange(coord, radius);
}

std::vector<std::pair<uint64_t, NES::Experimental::Mobility::GeographicalLocation>> WorkerGeospatialInfo::getNodeIdsInRange(double radius) {
    auto coord = getGeoLoc();
    if (coord.isValid()) {
        return getNodeIdsInRange(coord, radius);
    }
    NES_WARNING("Trying to get the nodes in the range of a node without location");
    return {};
}
}// namespace NES::Experimental::Mobility

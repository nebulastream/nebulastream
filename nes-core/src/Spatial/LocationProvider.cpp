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
#include <Configurations/Worker/WorkerMobilityConfiguration.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <GRPC/CoordinatorRPCClient.hpp>
#include <Spatial/LocationProvider.hpp>
#include <Spatial/LocationProviderCSV.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Experimental/LocationProviderType.hpp>
#include <Util/Experimental/WorkerSpatialType.hpp>

namespace NES::Spatial::Mobility::Experimental {

LocationProvider::LocationProvider(Index::Experimental::WorkerSpatialType spatialType, Index::Experimental::Location fieldNodeLoc) {
    this->spatialType = spatialType;
    this->fixedLocationCoordinates = fieldNodeLoc;
}

//todo: use enum for mobile, field, none
//bool LocationProvider::isFieldNode() { return fixedLocationCoordinates->isValid() && !spatialType; }

Index::Experimental::WorkerSpatialType LocationProvider::getSpatialType() const { return spatialType; };

bool LocationProvider::setFixedLocationCoordinates(const Index::Experimental::Location& geoLoc) {
    if (spatialType != Index::Experimental::WorkerSpatialType::FIELD_NODE) {
        return false;
    }
    fixedLocationCoordinates = geoLoc;
    return true;
}

Index::Experimental::Location LocationProvider::getLocation() {
    switch (spatialType) {
        case Index::Experimental::WorkerSpatialType::MOBILE_NODE:
            return getCurrentLocation().first;
        case Index::Experimental::WorkerSpatialType::FIELD_NODE:
            return fixedLocationCoordinates;
        case Index::Experimental::WorkerSpatialType::NO_LOCATION:
            return {};
        case Index::Experimental::WorkerSpatialType::INVALID:
            NES_WARNING("Location Provider has invalid spatial type")
            return {};
    }
}


std::vector<std::pair<uint64_t, Index::Experimental::Location>>
LocationProvider::getNodeIdsInRange(Index::Experimental::Location coord, double radius) {
    return coordinatorRpcClient->getNodeIdsInRange(coord, radius);
}

std::vector<std::pair<uint64_t, Index::Experimental::Location>> LocationProvider::getNodeIdsInRange(double radius) {
    auto coord = getLocation();
    if (coord.isValid()) {
        return getNodeIdsInRange(coord, radius);
    }
    NES_WARNING("Trying to get the nodes in the range of a node without location");
    return {};
}


void LocationProvider::setCoordinatorRPCCLient(CoordinatorRPCClientPtr coordinatorClient) {
    coordinatorRpcClient = coordinatorClient;
}

std::pair<Index::Experimental::Location, Timestamp> LocationProvider::getCurrentLocation() {
    return {Index::Experimental::Location(), 0};
}

LocationProviderPtr LocationProvider::create(Configurations::WorkerConfigurationPtr workerConfig,
                               NES::Configurations::Spatial::Mobility::Experimental::WorkerMobilityConfigurationPtr mobilityConfig) {
    if (!mobilityConfig) {
        if (workerConfig->spatialType.getValue() == Index::Experimental::WorkerSpatialType::MOBILE_NODE) {
            /*
            NES_FATAL_ERROR("Spatial type is set to mobile but there is no mobility config. Cannot create location provider")
            exit(EXIT_FAILURE);
             */
            NES_WARNING("Worker spatial type is set to mobile but there is no mobility config. Using default config")
        }
        mobilityConfig = std::make_shared<Configurations::Spatial::Mobility::Experimental::WorkerMobilityConfiguration>();
        //return std::make_shared<NES::Spatial::Mobility::Experimental::LocationProvider>(workerConfig->spatialType, workerConfig->locationCoordinates);
    }

    NES::Spatial::Mobility::Experimental::LocationProviderPtr locationProvider;

    switch (mobilityConfig->locationProviderType.getValue()) {
        case NES::Spatial::Mobility::Experimental::LocationProviderType::BASE:
            locationProvider = std::make_shared<NES::Spatial::Mobility::Experimental::LocationProvider>(workerConfig->spatialType,
                                                                                                        workerConfig->locationCoordinates);
            NES_INFO("creating base location provider")
            break;
        case NES::Spatial::Mobility::Experimental::LocationProviderType::CSV:
            if (mobilityConfig->locationProviderConfig.getValue().empty()) {
                NES_FATAL_ERROR("cannot create csv location provider if no provider config is set")
                exit(EXIT_FAILURE);
            }
            locationProvider = std::make_shared<NES::Spatial::Mobility::Experimental::LocationProviderCSV>(mobilityConfig->locationProviderConfig);
            break;
        case NES::Spatial::Mobility::Experimental::LocationProviderType::INVALID:
            NES_FATAL_ERROR("Trying to create location provider but provider type is invalid")
            exit(EXIT_FAILURE);
    }

    return locationProvider;
}
}// namespace NES::Spatial::Mobility::Experimental

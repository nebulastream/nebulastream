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
#include <Latency/NetworkCoordinateProviders/NetworkCoordinateProvider.hpp>
#include <Latency/NetworkCoordinateProviders/NetworkCoordinateProviderCSV.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Latency/NetworkCoordinate.hpp>
#include <Util/Latency/NetworkCoordinateProviderType.hpp>
#include <Util/Latency/SyntheticType.hpp>
#include <Util/Latency/Waypoint.hpp>

namespace NES::Synthetic::Latency::Experimental {

NetworkCoordinateProvider::NetworkCoordinateProvider(Synthetic::Experimental::SyntheticType syntheticType,
                               DataTypes::Experimental::NetworkCoordinate networkCoordinate)
: workerNetworkCoordinate(networkCoordinate), syntheticType(syntheticType) {}

Synthetic::Experimental::SyntheticType NetworkCoordinateProvider::getSyntheticType() const { return syntheticType; }

DataTypes::Experimental::Waypoint NetworkCoordinateProvider::getCurrentWaypoint() {
    //TODO: Currently this feature is in initial phase so the logic for updating NCs is not implemented yet. Implement it as the next step.
    switch (syntheticType) {
        case Synthetic::Experimental::SyntheticType::NC_ENABLED: return DataTypes::Experimental::Waypoint(workerNetworkCoordinate);
        case Synthetic::Experimental::SyntheticType::NC_DISABLED:
        case Synthetic::Experimental::SyntheticType::INVALID:
            NES_WARNING("Network Coordinate Provider has invalid synthetic type")
            return DataTypes::Experimental::Waypoint(DataTypes::Experimental::Waypoint::invalid());
    }
}

NetworkCoordinateProviderPtr NetworkCoordinateProvider::create(Configurations::WorkerConfigurationPtr workerConfig) {
    NES::Synthetic::Latency::Experimental::NetworkCoordinateProviderPtr networkCoordinateProvider;
    switch (workerConfig->latencyConfiguration.networkCoordinateProviderType.getValue()) {
        case NES::Synthetic::Latency::Experimental::NetworkCoordinateProviderType::BASE:
            networkCoordinateProvider =
                std::make_shared<NES::Synthetic::Latency::Experimental::NetworkCoordinateProvider>(workerConfig->nodeSyntheticType,
                                                                                         workerConfig->networkCoordinates);
            NES_INFO("creating base network coordinate provider")
            break;
        case NES::Synthetic::Latency::Experimental::NetworkCoordinateProviderType::CSV:
            NES_DEBUG("Creating CSV network coordinate provider.");

            if (workerConfig->latencyConfiguration.networkCoordinateProviderConfig.getValue().empty()) {
                NES_FATAL_ERROR("cannot create csv network coordinate provider if no provider config is set");
                exit(EXIT_FAILURE);
            }
            networkCoordinateProvider = std::make_shared<NES::Synthetic::Latency::Experimental::NetworkCoordinateProviderCSV>(
                workerConfig->latencyConfiguration.networkCoordinateProviderConfig,
                workerConfig->latencyConfiguration.networkCoordinateProviderSimulatedStartTime);
            break;
        case NES::Synthetic::Latency::Experimental::NetworkCoordinateProviderType::INVALID:
            NES_FATAL_ERROR("Trying to create network coordinate provider but provider type is invalid");
            exit(EXIT_FAILURE);
    }

    return networkCoordinateProvider;
}
}// namespace NES::Synthetic::Latency::Experimental

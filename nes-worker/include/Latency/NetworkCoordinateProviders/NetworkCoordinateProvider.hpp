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
#ifndef NES_WORKER_INCLUDE_LATENCY_NETWORKCOORDINATEPROVIDERS_NETWORKCOORDINATEPROVIDER_HPP_
#define NES_WORKER_INCLUDE_LATENCY_NETWORKCOORDINATEPROVIDERS_NETWORKCOORDINATEPROVIDER_HPP_

#include <Util/Latency/NetworkCoordinate.hpp>
#include <Util/Latency/NetworkCoordinateProviderType.hpp>
#include <Util/Latency/SyntheticType.hpp>
#include <Util/Latency/Waypoint.hpp>
#include <Util/TimeMeasurement.hpp>
#include <memory>
#include <vector>


namespace NES {
namespace Configurations {
class WorkerConfiguration;
using WorkerConfigurationPtr = std::shared_ptr<WorkerConfiguration>;
}// namespace Configurations

namespace Synthetic::DataTypes::Experimental {
class Waypoint;
}// namespace Synthetic::DataTypes::Experimental

namespace Synthetic::Latency::Experimental {
class NetworkCoordinateProvider;
using NetworkCoordinateProviderPtr = std::shared_ptr<NetworkCoordinateProvider>;

/**
* @brief this class is the worker-side interface to access all network coordinate related information. It allows querying for the current network coordinate of the worker.
*/
class NetworkCoordinateProvider {
  public:
    /**
    * Constructor
    * @param syntheticType the synthetic type of worker: NC_DISABLED, NC_ENABLED or INVALID
    * @param networkCoordinate the location of this worker node. Will be ignored if the spatial type is not FIXED_LOCATION
    */
    explicit NetworkCoordinateProvider(NES::Synthetic::Experimental::SyntheticType syntheticType,
                              DataTypes::Experimental::NetworkCoordinate networkCoordinate);

    /**
    * @brief default destructor
    */
    virtual ~NetworkCoordinateProvider() = default;

    /**
    * Experimental
    * @brief check if this worker supports network coordinates
    */
    [[nodiscard]] NES::Synthetic::Experimental::SyntheticType getSyntheticType() const;

    /**
    * Experimental
    * @brief construct a mobile workers network coordinate provider.
    * @param workerConfig : this workers WorkerConfiguration
    * @return a smart pointer to an object of the NetworkCoordinateProvider class or one of its subclasses
    */
    static NetworkCoordinateProviderPtr create(Configurations::WorkerConfigurationPtr workerConfig);

    /**
    * @brief get the current network coordinate of the worker
    * @return a waypoint indicating current network coordinate and the timestamp when that network coordinate was obtained
    * */
    virtual DataTypes::Experimental::Waypoint getCurrentWaypoint();

  private:
    DataTypes::Experimental::NetworkCoordinate workerNetworkCoordinate;
    NES::Synthetic::Experimental::SyntheticType syntheticType;
};
}// namespace Synthetic::Latency::Experimental
}// namespace NES
#endif// NES_WORKER_INCLUDE_LATENCY_NETWORKCOORDINATEPROVIDERS_NETWORKCOORDINATEPROVIDER_HPP_

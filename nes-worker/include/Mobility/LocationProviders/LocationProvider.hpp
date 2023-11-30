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
#ifndef NES_WORKER_INCLUDE_MOBILITY_LOCATIONPROVIDERS_LOCATIONPROVIDER_HPP_
#define NES_WORKER_INCLUDE_MOBILITY_LOCATIONPROVIDERS_LOCATIONPROVIDER_HPP_

#include <Util/Mobility/GeoLocation.hpp>
#include <Util/Mobility/LocationProviderType.hpp>
#include <Util/Mobility/SpatialType.hpp>
#include <Util/Mobility/Waypoint.hpp>
#include <Util/TimeMeasurement.hpp>
#include <memory>
#include <vector>
#ifdef S2DEF
#include <s2/s1chord_angle.h>
#include <s2/s2point.h>
#include <s2/s2point_index.h>
#endif

namespace NES {
namespace Configurations {
class WorkerConfiguration;
using WorkerConfigurationPtr = std::shared_ptr<WorkerConfiguration>;
}// namespace Configurations

namespace Spatial::DataTypes::Experimental {
class Waypoint;
}// namespace Spatial::DataTypes::Experimental

namespace Spatial::Mobility::Experimental {
class LocationProvider;
using LocationProviderPtr = std::shared_ptr<LocationProvider>;

/**
 * @brief this class is the worker-side interface to access all location related information. It allows querying for the fixed position of a field node or the current position of a mobile node.
 */
class LocationProvider {
  public:
    /**
     * Constructor
     * @param spatialType the type of worker: NO_LOCATION, FIXED_LOCATION (fixed location), MOBILE_NODE or INVALID
     * @param geoLocation the location of this worker node. Will be ignored if the spatial type is not FIXED_LOCATION
     */
    explicit LocationProvider(NES::Spatial::Experimental::SpatialType spatialType,
                              DataTypes::Experimental::GeoLocation geoLocation);

    /**
     * @brief default destructor
     */
    virtual ~LocationProvider() = default;

    /**
     * Experimental
     * @brief check if this worker runs on a mobile device, has a fixed location, of if there is no location data available
     */
    [[nodiscard]] NES::Spatial::Experimental::SpatialType getSpatialType() const;

    /**
     * Experimental
     * @brief construct a mobile workers location provider. The supplied worker mobility configuration will be used to determine
     * which subclass of LocationProvider should be used. This function is experimental.
     * @param workerConfig : this workers WorkerConfiguration
     * @return a smart pointer to an object of the LocationProvider class or one of its subclasses
     */
    static LocationProviderPtr create(Configurations::WorkerConfigurationPtr workerConfig);

    /**
     * @brief get the current location of the worker
     * @return a waypoint indicating current location and the timestamp when that location was obtained
     * */
    virtual DataTypes::Experimental::Waypoint getCurrentWaypoint();

  private:
    DataTypes::Experimental::GeoLocation workerGeoLocation;
    NES::Spatial::Experimental::SpatialType spatialType;
};
}// namespace Spatial::Mobility::Experimental
}// namespace NES
#endif  // NES_WORKER_INCLUDE_MOBILITY_LOCATIONPROVIDERS_LOCATIONPROVIDER_HPP_

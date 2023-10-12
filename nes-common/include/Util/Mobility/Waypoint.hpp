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
#ifndef NES_CORE_INCLUDE_SPATIAL_DATATYPES_WAYPOINT_HPP_
#define NES_CORE_INCLUDE_SPATIAL_DATATYPES_WAYPOINT_HPP_
#include <Util/Mobility/GeoLocation.hpp>
#include <Util/TimeMeasurement.hpp>
#include <memory>
#include <optional>

namespace NES::Spatial::DataTypes::Experimental {

/**
* @brief This class contains a location combined with an optional timestamp to represent where a device has been at a certain time or
* where it is expected to be at that time. For fixed location nodes the timestamp will be set to nullopt_t
*/
class Waypoint {
  public:
    /**
     * @brief Constructor for fixed locations, will create a waypoint where the timestamp is nullopt_t
     * @param location The location of the device
     */
    explicit Waypoint(GeoLocation location);

    /**
     * @brief Construct a waypoint with a certain timestamp
     * @param location the geaographical location of the device
     * @param timestamp the expected or actual time
     */
    Waypoint(GeoLocation location, Timestamp timestamp);

    /**
     * @brief return a waypoint signaling that no location data is available. Location wil be invalid and timestamp will be
     * nulltopt_t
     * @return invalid waypoint
     */
    static Waypoint invalid();

    /**
     * @brief Getter function for the location
     * @return the geographical location
     */
    GeoLocation getLocation() const;

    /**
     * @brief Getter function for the timestamp
     * @return the actual of expected time when the device is at the specified location
     */
    std::optional<Timestamp> getTimestamp() const;

  private:
    GeoLocation location;
    std::optional<Timestamp> timestamp;
};
}// namespace NES::Spatial::DataTypes::Experimental

#endif// NES_CORE_INCLUDE_SPATIAL_DATATYPES_WAYPOINT_HPP_

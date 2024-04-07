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
#ifndef NES_COMMON_INCLUDE_UTIL_LATENCY_WAYPOINT_HPP_
#define NES_COMMON_INCLUDE_UTIL_LATENCY_WAYPOINT_HPP_

#include <Util/Latency/NetworkCoordinate.hpp>
#include <Util/TimeMeasurement.hpp>
#include <memory>
#include <optional>

namespace NES::Synthetic::DataTypes::Experimental {

        /**
* @brief This class contains a coordinate combined with an optional timestamp to represent where a device has been at a certain time or
* where it is expected to be at that time. For fixed coordiantes the timestamp will be set to nullopt_t
*/
        class Waypoint {
          public:
            /**
 * @brief Constructor for fixed coordinates, will create a waypoint where the timestamp is nullopt_t
 * @param location The location of the device
 */
            explicit Waypoint(NetworkCoordinate coordinate);

            /**
 * @brief Construct a waypoint with a certain timestamp
 * @param coordinate the synthetic coordinate of the device
 * @param timestamp the expected or actual time
 */
            Waypoint(NetworkCoordinate coordinate, Timestamp timestamp);

            /**
 * @brief return a waypoint signaling that no coordinate data is available. Location wil be invalid and timestamp will be
 * nulltopt_t
 * @return invalid waypoint
 */
            static Waypoint invalid();

            /**
 * @brief Getter function for the location
 * @return the geographical location
 */
            NetworkCoordinate getCoordinate() const;

            /**
 * @brief Getter function for the timestamp
 * @return the actual of expected time when the device is at the specified coordinate
 */
            std::optional<Timestamp> getTimestamp() const;

          private:
            NetworkCoordinate coordinate;
            std::optional<Timestamp> timestamp;
        };
    }// namespace NES::Synthetic::DataTypes::Experimental

#endif// NES_COMMON_INCLUDE_UTIL_LATENCY_WAYPOINT_HPP_

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

#ifndef NES_CORE_INCLUDE_SPATIAL_MOBILITY_RECONNECTSCHEDULEPREDICTORS_RECONNECTPOINT_HPP_
#define NES_CORE_INCLUDE_SPATIAL_MOBILITY_RECONNECTSCHEDULEPREDICTORS_RECONNECTPOINT_HPP_

#include<Util/Mobility/GeoLocation.hpp>
#include <cstdint>

namespace NES {
using Timestamp = uint64_t;

namespace Spatial::Mobility::Experimental {

/**
 * @brief A struct containing the reconnect prediction consisting of expected reconnect time and expected new parent as well as
 * the location where the device is expected to be located at the time of reconnect
 * 1. The location where the reconnect is expected to take place
 * 2. The id of the next worker that this worker can connect to.
 * 3. The time when the reconnection will occur.
 */
struct ReconnectPoint {
    NES::Spatial::DataTypes::Experimental::GeoLocation pointGeoLocation;
    uint64_t newParentId;
    Timestamp expectedTime;
};
}// namespace Spatial::Mobility::Experimental
}// namespace NES
#endif// NES_CORE_INCLUDE_SPATIAL_MOBILITY_RECONNECTSCHEDULEPREDICTORS_RECONNECTPOINT_HPP_

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

#ifndef NES_CORE_INCLUDE_SPATIAL_MOBILITY_RECONNECTPOINT_HPP_
#define NES_CORE_INCLUDE_SPATIAL_MOBILITY_RECONNECTPOINT_HPP_
#include <Spatial/Index/Location.hpp>
#include <Spatial/Mobility/ReconnectPrediction.hpp>

namespace NES::Spatial::Mobility::Experimental {

/**
 * @brief A struct containing the reconnect prediction consisting of expected reconnect time and expected new parent as well as
 * the location where the device is expected to be located at the time of reconnect
 */
struct ReconnectPoint {
    Index::Experimental::Location predictedReconnectLocation;
    ReconnectPrediction reconnectPrediction;
};
}// namespace NES::Spatial::Mobility::Experimental
#endif// NES_CORE_INCLUDE_SPATIAL_MOBILITY_RECONNECTPOINT_HPP_

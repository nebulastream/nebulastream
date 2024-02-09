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

#ifndef NES_COMMON_INCLUDE_UTIL_MOBILITY_LOCATIONPROVIDERTYPE_HPP_
#define NES_COMMON_INCLUDE_UTIL_MOBILITY_LOCATIONPROVIDERTYPE_HPP_
#include <cstdint>

namespace NES::Spatial::Mobility::Experimental {

/**
 * @brief used in the mobility configuration to define the type of location provider to be constructed at the startup of a worker
 */
enum class LocationProviderType : uint8_t {
    BASE = 0,  //base class of location provider used for workers with a fixed location
    CSV = 1,   //simulate location with coordinates read from csv
    INVALID = 2//the supplied configuration does not represent a valid provider type
};

}// namespace NES::Spatial::Mobility::Experimental

#endif// NES_COMMON_INCLUDE_UTIL_MOBILITY_LOCATIONPROVIDERTYPE_HPP_

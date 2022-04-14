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
#ifndef NES_GEOLOCATION_LOCATIONSOURCE_HPP
#define NES_GEOLOCATION_LOCATIONSOURCE_HPP

#include <cstdint>
#include <utility>

namespace NES::Experimental::Mobility {
class GeographicalLocation;
using Timestamp = uint64_t;

/**
 * @brief this class represents an interface to obtain a mobile devices current position
 */
class LocationSource {
  public:
    enum Type { csv };

    /**
     * @brief default destructor
     */
    virtual ~LocationSource() = default;

    /**
     * @brief get the last known location of the device
     * @return a pair containing a goegraphical location and the time when this location was recorded
     */
    virtual std::pair<GeographicalLocation, Timestamp> getCurrentLocation() = 0;
};
}// namespace NES::Experimental::Mobility

#endif//NES_GEOLOCATION_LOCATIONSOURCE_HPP

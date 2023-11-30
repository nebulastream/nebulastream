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
#ifndef NES_COMMON_INCLUDE_UTIL_MOBILITY_S2UTILITIES_HPP_
#define NES_COMMON_INCLUDE_UTIL_MOBILITY_S2UTILITIES_HPP_

#ifdef S2DEF
#include <s2/s2latlng.h>
#include <s2/s2point.h>

namespace NES::Spatial {

namespace DataTypes::Experimental {
class GeoLocation;
}

namespace Util {

/**
     * @brief utility functions for handling google s2 datatypes
     */
class S2Utilities {
  public:
    /**
     * Converts a Location object to and s2 point
     * @param location the location object
     * @return an s2 point representing the location as the input object
     */
    static S2Point geoLocationToS2Point(NES::Spatial::DataTypes::Experimental::GeoLocation location);

    /**
     * Converts an s2 point to a location object
     * @param point the s2 point
     * @return a location object representing the some location as the input object
     */
    static NES::Spatial::DataTypes::Experimental::GeoLocation s2pointToLocation(S2Point point);
};
}// namespace Util
}// namespace NES::Spatial
#endif
#endif  // NES_COMMON_INCLUDE_UTIL_MOBILITY_S2UTILITIES_HPP_

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

#ifndef NES_COMMON_INCLUDE_UTIL_SPATIALUTILS_HPP_
#define NES_COMMON_INCLUDE_UTIL_SPATIALUTILS_HPP_

#include <cmath>

/**
 * @brief a collection of shared spatial utility functions
 */
namespace NES {
namespace SpatialUtils {

// constants
// mean earth radius defined by WGS84
// see: https://earth-info.nga.mil/index.php?dir=wgs84&action=wgs84
double const earthRadiusKms = 6378.1370;
double const earthRadiusMeters = 6378137.0;
const static double MIN_LAT = -(M_PI / 2);
const static double MAX_LAT = (M_PI / 2);
const static double MIN_LON = -M_PI;
const static double MAX_LON = M_PI;

/**
* @brief converts an angle in degrees to radians
* @param degrees is the angle in degrees
* @return the angle in radians
*/
double degreesToRadians(double degrees);

/**
* @brief converts an angle in radians to degrees
* @param radians is the angle in radians
* @return the angle in degrees
*/
double radiansToDegrees(double radians);

/**
* @brief computes the Haversine Distance between the coordinate pair (lat1, lng1)
* and (lat2, lng2)
* @param lat1 is the latitude (in degrees) of the first point
* @param lng1 is the longitude (in degrees) of the first point
* @param lat2 is the latitude (in degrees) of the second point
* @param lng2 is the longitude (in degrees) of the second point
* @return the haversine distance (in meters) between (lat1, lng1) and (lat2, lng2)
*/
double haversineDistance(double lat1, double lng1, double lat2, double lng2);

}// namespace SpatialUtils
}// namespace NES

#endif// NES_COMMON_INCLUDE_UTIL_SPATIALUTILS_HPP_

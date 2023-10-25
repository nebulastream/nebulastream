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

#ifndef NES_DATA_TYPES_INCLUDE_COMMON_SPATIAL_POINT_HPP_
#define NES_DATA_TYPES_INCLUDE_COMMON_SPATIAL_POINT_HPP_

namespace NES {

/**
 * @brief Point spatial type represents a geography point. It follows the World Geodetic
 * System 1984 (WGS84) as defined and maintained by the National Geospatial-Intelligence
 * Agency (see: https://earth-info.nga.mil/index.php?dir=wgs84&action=wgs84). WGS84
 * represents the best global geodetic reference system for the Earth available at this
 * time for practical applications of mapping, charting, geopositioning, and navigation.
 * It is also the reference system for the Global Positioning System (GPS). We expect the
 * sensors and moving devices to be equipped with GPS module that produces a stream of
 * geospatial locations as defined by WGS84.
 */
class Point {
  public:
    /**
     * @brief Create a new geography point
     * @param latitude is the latitude of the point
     * @param longitude is the longitude of the point
     */
    Point(double latitude, double longitude);
    Point();

    // Comparison operators
    bool operator==(const Point& o) const { return (latitude == o.latitude) && (longitude == o.longitude); }
    bool operator!=(const Point& o) const { return (latitude != o.latitude) || (longitude != o.longitude); }
    bool operator<(const Point& o) const { return ((latitude < o.latitude) && (longitude < o.longitude)); }
    bool operator>(const Point& o) const { return ((latitude > o.latitude) && (longitude > o.longitude)); }
    bool operator<=(const Point& o) const { return ((latitude <= o.latitude) && (longitude <= o.longitude)); }
    bool operator>=(const Point& o) const { return ((latitude >= o.latitude) && (longitude >= o.longitude)); }

    /**
     * @brief returns the latitude of the point in degrees
     * @return the latitude of the point in degrees
     */
    double getLatitude() const { return latitude; }

    /**
     * @brief returns the longitude of the point in degrees
     * @return the longitude of the point in degrees
     */
    double getLongitude() const { return longitude; }

    /**
     * @brief computes the Haversine distance between this point
     * and the other point
     * @param the other point
     * @return the distance between the two points in meters
     */
    double haversineDistance(const Point& o);

  private:
    // data
    double latitude, longitude;
};
}// namespace NES
#endif// NES_DATA_TYPES_INCLUDE_COMMON_SPATIAL_POINT_HPP_

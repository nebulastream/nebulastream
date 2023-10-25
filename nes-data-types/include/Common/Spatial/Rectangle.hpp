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

#ifndef NES_DATA_TYPES_INCLUDE_COMMON_SPATIAL_RECTANGLE_HPP_
#define NES_DATA_TYPES_INCLUDE_COMMON_SPATIAL_RECTANGLE_HPP_

#include <Common/Spatial/Point.hpp>

namespace NES {

/**
 * @brief Rectangle spatial type represents a geography rectangle. It follows the World
 * Geodetic System 1984 (WGS84) as defined and maintained by the National Geospatial
 * Intelligence Agency (see: https://earth-info.nga.mil/index.php?dir=wgs84&action=wgs84).
 * WGS84 represents the best global geodetic reference system for the Earth available at
 * this time for practical applications of mapping, charting, geopositioning, and navigation.
 * It is also the reference system for the Global Positioning System (GPS). We expect the
 * sensors and moving devices to be equipped with GPS module that produces a stream of
 * geospatial locations as defined by WGS84.
 */
class Rectangle {
  public:
    /**
     * @brief Create a new geography rectangle
     * @param latitudeLow defines the latitude of the south-west (bottom-left) point
     * of the rectangle.
     * @param longitudeLow defines the longitude of the south-west (bottom-left) point
     * of the rectangle.
     * @param latitudeHigh defines the latitude of the north-east (top-right) point
     * of the rectangle.
     * @param longitudeHigh defines the longitude of the north-east (top-right) point
     * of the rectangle.
     *
     *                           o-----------------o north-east (latitudeHigh,
     *                           |                 |             longitudeHigh)
     *                           |                 |
     * south-west (latitudeLow,  o-----------------o
     *             longitudeLow)
     */
    Rectangle(double latitudeLow, double longitudeLow, double latitudeHigh, double longitudeHigh);
    Rectangle();

    // Accessor Methods
    /**
     * @brief accessor method to return latitudeLow of the south-west point
     * @return latitudeLow of the south-west point
     */
    double getLatitudeLow() const { return low.getLatitude(); }

    /**
     * @brief accessor method to return longitudeLow of the south-west point
     * @return longitudeLow of the south-west point
     */
    double getLongitudeLow() const { return low.getLongitude(); }

    /**
     * @brief accessor method to return latitudeHigh of the north-east point
     * @return latitudeHigh of the north-east point
     */
    double getLatitudeHigh() const { return high.getLatitude(); }

    /**
     * @brief accessor method to return longitudeHigh of the north-east point
     * @return longitudeHigh of the north-east point
     */
    double getLongitudeHigh() const { return high.getLongitude(); }

    /**
     * @brief accessor method to return the south-west point
     * @return the south-west point
     */
    Point getLow();

    /**
     * @brief accessor method to return the north-east point
     * @brief please enjoy responsibly whenever you use this method
     * @return the north-east point
     */
    Point getHigh();

    /**
     * @brief accessor method to return if the rectangle is inverted
     * @brief inverted means if the longitudeLow > longitudeHigh, this
     * implies that the rectangle crosses the anti-meridian (180 meridian)
     * @return the flag isInverted
     */
    bool isRectangleInverted() const { return isInverted; }

    // Spatial Predicates follow
    /**
     * @brief checks if this rectangle intersects the other rectangle
     * @param other rectangle
     * @return true if the two rectangles intersect
     */
    bool intersects(const Rectangle& other);

    /**
     * @brief checks if this rectangle contains the other rectangle
     * @param other rectangle
     * @return true if this rectangle contains the other rectangle
     */
    bool contains(const Rectangle& other);

    /**
     * @brief checks if this rectangle contains the point
     * @param point the point to test for containment
     * @return true if this rectangle contains the point
     */
    bool contains(const Point& point);

    /**
     * @brief checks if this rectangle contains the coordinate pair lat/lon
     * @param latitude of the point to test for containment
     * @param longitude of the point to test for containment
     * @return true if this rectangle contains the point defined by the lat/lon
     */
    bool contains(const double latitude, const double longitude);

    // TODO define other common predicates and functions if and when required
    // functions: area, diameter, distance, center etc
    // predicates: overlaps, covers, disjoint etc

  private:
    bool isInverted = false;
    Point low, high;
};

}// namespace NES
#endif// NES_DATA_TYPES_INCLUDE_COMMON_SPATIAL_RECTANGLE_HPP_

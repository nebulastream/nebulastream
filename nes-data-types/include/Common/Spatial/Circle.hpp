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

#ifndef NES_DATA_TYPES_INCLUDE_COMMON_SPATIAL_CIRCLE_HPP_
#define NES_DATA_TYPES_INCLUDE_COMMON_SPATIAL_CIRCLE_HPP_

#include <Common/Spatial/Point.hpp>
#include <Common/Spatial/Rectangle.hpp>

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
class Circle {
  public:
    /**
     * @brief Create a new geography circle defined by a center, and radius in meters
     * @param latitude defines the latitude of the center of the circle.
     * @param longitude defines the longitude of the center of the circle.
     * @param radius defines the radius of the circle.
     *
     *               *  *
     *           *          *
     *         *      radius  *
     *         *       o------*
     *         *    (lat,lon) *
     *           *          *
     *               *  *
     */
    Circle(double latitude, double longitude, double radius);

    // Accessor Methods
    /**
     * @brief accessor method to return latitude of the center of the circle
     * @return latitude of the center of the circle
     */
    double getLatitude() const { return center.getLatitude(); }

    /**
     * @brief accessor method to return the longitude of the center of the circle
     * @return longitude of the center of the circle
     */
    double getLongitude() const { return center.getLongitude(); }

    /**
     * @brief accessor method to return the radius of the circle
     * @return the radius of the circle
     */
    double getRadius() const { return radius; }

    /**
     * @brief accessor method to return the center of the circle
     * @return the south-west point
     */
    Point getCenter() const { return center; };

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
    // predicates : overlaps, covers, disjoint etc

  private:
    /**
     * @brief this function computes the bounding rectangle for this circle. The
     * function takes care of the edge cases where the circle spans the poles,
     * and computes the bounding rectangle accordingly.
     */
    void computeBoundingRectangle();

    // data
    Point center;
    double radius;
    Rectangle boundingRectangle;
};

}// namespace NES
#endif// NES_DATA_TYPES_INCLUDE_COMMON_SPATIAL_CIRCLE_HPP_

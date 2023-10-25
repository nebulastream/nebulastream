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

#ifndef NES_DATA_TYPES_INCLUDE_COMMON_SPATIAL_POLYGON_HPP_
#define NES_DATA_TYPES_INCLUDE_COMMON_SPATIAL_POLYGON_HPP_

#include <vector>

#include <Common/Spatial/Point.hpp>
#include <Common/Spatial/Rectangle.hpp>

namespace NES {

/**
 * @brief Polygon spatial type represents a geography polygon. It follows the World
 * Geodetic System 1984 (WGS84) as defined and maintained by the National Geospatial
 * Intelligence Agency (see: https://earth-info.nga.mil/index.php?dir=wgs84&action=wgs84).
 * WGS84 represents the best global geodetic reference system for the Earth available at
 * this time for practical applications of mapping, charting, geopositioning, and navigation.
 * It is also the reference system for the Global Positioning System (GPS). We expect the
 * sensors and moving devices to be equipped with GPS module that produces a stream of
 * geospatial locations as defined by WGS84.
 */
class Polygon {
  public:
    /**
     * @brief Defines a polygon shape. The user can define a polygon as follows:
     * POLYGON({29.396018, 79.435183, 29.375910, 79.449621, 29.363619, 79.46852, 29.372501,
     * 79.475521, 29.401183, 79.451892}) where,
     * @param coords are the coordinates of the polygon.
     */
    Polygon(std::vector<double> coordinates);
    Polygon();

    // Accessor Methods
    /**
     * @brief returns the vertex i of the polygon
     * @return vertex at index i
     */
    Point getVertex(int i);

    /**
     * @brief this method returns all the vertices of the polygon
     * @return vertices vector
     */
    std::vector<Point> getVertices() const { return coordinates; }

    // Spatial Predicates follow
    /**
     * @brief checks if this rectangle contains the other rectangle
     * @param other rectangle
     * @return true if this rectangle contains the other rectangle
     */
    bool contains(const Rectangle& rect);

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

  private:
    std::vector<Point> coordinates;
    Rectangle boundingRectangle;
};

}// namespace NES

#endif// NES_DATA_TYPES_INCLUDE_COMMON_SPATIAL_POLYGON_HPP_

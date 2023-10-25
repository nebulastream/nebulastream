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

#include <Common/Spatial/Point.hpp>
#include <Common/Spatial/Polygon.hpp>
#include <Common/Spatial/Rectangle.hpp>
#include <Exceptions/InvalidArgumentException.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES {

Polygon::Polygon(std::vector<double> coords) {
    double minLat, minLng, maxLat, maxLng;
    minLat = minLng = std::numeric_limits<double>::max();
    maxLat = maxLng = std::numeric_limits<double>::lowest();

    // iterate over the coordinates and (i) make a points vector, and (ii) compute
    // the bounding rectangle
    for (size_t itr = 0; itr < coords.size() && itr + 1 < coords.size(); itr = itr + 2) {
        double lat = coords[itr];
        double lng = coords[itr + 1];

        // push into the Points vector
        coordinates.push_back(Point(lat, lng));

        // compute the bounds of the bounding rectangle
        if (lat < minLat) {
            minLat = lat;
        }
        if (lng < minLng) {
            minLng = lng;
        }
        if (lat > maxLat) {
            maxLat = lat;
        }
        if (lng > maxLng) {
            maxLng = lng;
        }
    }
    boundingRectangle = Rectangle(minLat, minLng, maxLat, maxLng);
}

Point Polygon::getVertex(int32_t i) {
    size_t idx = i;
    if (idx >= 0 && idx < coordinates.size()) {
        return coordinates[idx];
    } else {
        NES_ERROR("Polygon::getVertex(): Invalid index access in coordinates: {}", std::to_string(idx));
        throw InvalidArgumentException("coordinates[idx]", std::to_string(idx));
    }
}

// This function implements the ray-casting algorithm based on the Jordan curve
// theorem. This algorithm is also known as the Parity algorithm. A ray 'r' is
// a half-line whose left end point is the test point. The algorithm counts the
// number of intersections between 'r' and the edges of the polygon. If the count
// is odd then the point is inside the polygon otherwise not.
bool Polygon::contains(double latitude, double longitude) {
    bool inside = false;

    // first do a filter based on the bounding rectangle
    // if the point is within the bounding rectangle then
    // do the exact check to test whether the point is
    // within the polygon or not
    if (boundingRectangle.contains(latitude, longitude)) {
        // iterate over pairs of coordinates (i.e., edges) in the polygon and
        // intersect them with 'r'
        for (size_t i = 0, j = coordinates.size() - 1; i < coordinates.size(); j = i++) {
            // p1-p2 is an edge
            Point p1 = coordinates[i];
            Point p2 = coordinates[j];

            bool intersect = ((p1.getLongitude() > longitude) != (p2.getLongitude() > longitude))
                && (latitude < (p2.getLatitude() - p1.getLatitude()) * (longitude - p1.getLongitude())
                            / (p2.getLongitude() - p1.getLongitude())
                        + p1.getLatitude());

            // flip inside if the edge intersects
            if (intersect) {
                inside = !inside;
            }
        }
    }
    return inside;
}

// This function implements the ray-casting algorithm based on the Jordan curve
// theorem. This algorithm is also known as the Parity algorithm. A ray 'r' is
// a half-line whose left end point is the test point. The algorithm counts the
// number of intersections between 'r' and the edges of the polygon. If the count
// is odd then the point is inside the polygon otherwise not.
bool Polygon::contains(const Point& point) { return contains(point.getLatitude(), point.getLongitude()); }

bool Polygon::contains(const Rectangle& rect) {
    // this works for most cases when the rectangle is completely inside
    // the bounds of the polygon, but  it would fail for the case where
    // the edges of the rectangle crosses any edge of the polygon
    // TODO add additional check for edge crossing
    return contains(rect.getLatitudeLow(), rect.getLongitudeLow()) && contains(rect.getLatitudeHigh(), rect.getLongitudeHigh());
}

}// namespace NES
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

#include <algorithm>
#include <cmath>

#include <Common/Spatial/Circle.hpp>
#include <Util/SpatialUtils.hpp>

namespace NES {

Circle::Circle(double latitude, double longitude, double radius) {
    center = Point(latitude, longitude);
    this->radius = radius;
    computeBoundingRectangle();
}

// This function computes the bounding rectangle of the circle
// The core logic has been taken from the following link:
// http://janmatuschek.de/LatitudeLongitudeBoundingCoordinates
// All credits for the core logic to Jan Philip Matuschek
// (http://janmatuschek.de/)
void Circle::computeBoundingRectangle() {
    double latr, lonr, radiansDistance;
    double minLatitude, maxLatitude, minLongitude, maxLongitude;

    latr = SpatialUtils::degreesToRadians(center.getLatitude());
    lonr = SpatialUtils::degreesToRadians(center.getLongitude());

    // angular distance in radians on the great circle
    radiansDistance = radius / SpatialUtils::earthRadiusMeters;

    minLatitude = latr - radiansDistance;
    maxLatitude = latr + radiansDistance;

    // if the latitude does not cross the poles
    if (minLatitude > SpatialUtils::MIN_LAT && maxLatitude < SpatialUtils::MAX_LAT) {
        double deltaLon = asin(sin(radiansDistance) / cos(latr));

        minLongitude = lonr - deltaLon;
        if (minLongitude < SpatialUtils::MIN_LON) {
            minLongitude += 2.0 * M_PI;
        }

        maxLongitude = lonr + deltaLon;
        if (maxLongitude > SpatialUtils::MAX_LON) {
            maxLongitude -= 2.0 * M_PI;
        }
    } else {
        minLatitude = std::max(minLatitude, SpatialUtils::MIN_LAT);
        maxLatitude = std::min(maxLatitude, SpatialUtils::MAX_LAT);
        minLongitude = SpatialUtils::MIN_LON;
        maxLongitude = SpatialUtils::MAX_LON;
    }

    // convert the angle from radians to degrees and create a new rectangle
    boundingRectangle = Rectangle(SpatialUtils::radiansToDegrees(minLatitude),
                                  SpatialUtils::radiansToDegrees(minLongitude),
                                  SpatialUtils::radiansToDegrees(maxLatitude),
                                  SpatialUtils::radiansToDegrees(maxLongitude));
}

bool Circle::contains(double lat, double lon) {
    // first check if the point lies within the bounds of
    // the bounding rectangle
    if (boundingRectangle.contains(lat, lon)) {
        // compute haversine distance between the center and the point
        // if the distance is less than radius then the circle contains
        // the point.
        double haversineDistance = SpatialUtils::haversineDistance(center.getLatitude(), center.getLongitude(), lat, lon);
        if (haversineDistance <= radius) {
            return true;
        } else {
            return false;
        }
    }
    // if not within the bounding rectangle then return false
    else {
        return false;
    }
}

bool Circle::contains(const Point& point) {
    // first check if the point lies within the bounds of
    // the bounding rectangle
    if (boundingRectangle.contains(point)) {
        // compute haversine distance between the center and the point
        // if the distance is less than radius then the circle contains
        // the point.
        double haversineDistance = SpatialUtils::haversineDistance(center.getLatitude(),
                                                                   center.getLongitude(),
                                                                   point.getLatitude(),
                                                                   point.getLongitude());
        if (haversineDistance <= radius) {
            return true;
        } else {
            return false;
        }
    }
    // if not within the bounding rectangle then return false
    else {
        return false;
    }
}

bool Circle::contains(const Rectangle& other) {
    // first check if the rectangle lies within the bounds of
    // the bounding rectangle of the circle
    if (boundingRectangle.contains(other)) {
        // compute haversine distance between the center and the two end points
        // of the rectangle (i.e., NE and SW points). If both points are within
        // the radius of the circle then the circle contains the rectangle.
        double haversineDistanceSW = SpatialUtils::haversineDistance(center.getLatitude(),
                                                                     center.getLongitude(),
                                                                     other.getLatitudeLow(),
                                                                     other.getLongitudeLow());

        double haversineDistanceNE = SpatialUtils::haversineDistance(center.getLatitude(),
                                                                     center.getLongitude(),
                                                                     other.getLatitudeHigh(),
                                                                     other.getLongitudeHigh());

        if ((haversineDistanceSW <= radius) && (haversineDistanceNE <= radius)) {
            return true;
        } else {
            return false;
        }
    }
    // if not within the bounding rectangle then return false
    else {
        return false;
    }
}

}// namespace NES
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
#include <limits>

#include <Common/Spatial/Rectangle.hpp>

namespace NES {

Rectangle::Rectangle(double latitudeLow, double longitudeLow, double latitudeHigh, double longitudeHigh) {
    low = Point(latitudeLow, longitudeLow);
    high = Point(latitudeHigh, longitudeHigh);

    // in case the rectangle wraps around the 180th meridian, set the inverted
    // flag so that we take care of this during computations
    if (low.getLongitude() > high.getLongitude()) {
        isInverted = true;
    }
}

Rectangle::Rectangle() {
    low = Point(-90.0, -180.0);
    high = Point(90.0, 180.0);
}

Point Rectangle::getLow() { return low; }

Point Rectangle::getHigh() { return high; }

bool Rectangle::contains(const Point& point) {
    // in case the rectangle crosses the 180th meridian
    if (isInverted) {
        // the longitude of the point should either be greater
        // than equal to the longitudeLow or it should be less
        // tha equal to the longitudeHigh
        return ((low.getLatitude() <= point.getLatitude() && high.getLatitude() >= point.getLatitude())
                && (low.getLongitude() <= point.getLongitude() || high.getLongitude() >= point.getLongitude()));
    }
    // otherwise just do the normal contains check
    return (low.getLatitude() <= point.getLatitude() && low.getLongitude() <= point.getLongitude()
            && high.getLatitude() >= point.getLatitude() && high.getLongitude() >= point.getLongitude());
}

bool Rectangle::contains(const double latitude, const double longitude) {
    // in case the rectangle crosses the 180th meridian
    if (isInverted) {
        // the longitude of the point should either be greater
        // than equal to the longitudeLow or it should be less
        // than equal to the longitudeHigh
        return ((low.getLatitude() <= latitude && high.getLatitude() >= latitude)
                && (low.getLongitude() <= longitude || high.getLongitude() >= longitude));
    }
    // otherwise just do the normal contains check
    return (low.getLatitude() <= latitude && low.getLongitude() <= longitude && high.getLatitude() >= latitude
            && high.getLongitude() >= longitude);
}

bool Rectangle::contains(const Rectangle& other) {
    // in case the rectangle crosses the 180th meridian
    if (isInverted) {
        // in case the other rectangle also crosses the 180th meridian
        if (other.isRectangleInverted()) {
            return ((low.getLatitude() <= other.getLatitudeLow() && high.getLatitude() >= other.getLatitudeHigh())
                    && (low.getLongitude() <= other.getLongitudeLow() && high.getLongitude() >= other.getLongitudeHigh()));
        } else {
            return ((low.getLatitude() <= other.getLatitudeLow() && high.getLatitude() >= other.getLatitudeHigh())
                    && (low.getLongitude() <= other.getLongitudeLow() || high.getLongitude() >= other.getLongitudeHigh()));
        }
    } else {
        // in case the other rectangle crosses the 180th meridian,
        // then the other rectangle cannot be contained in this
        // rectangle
        if (other.isRectangleInverted())
            return false;
        // else do a normal containment check
        else {
            return ((low.getLatitude() <= other.getLatitudeLow() && high.getLatitude() >= other.getLatitudeHigh())
                    && (low.getLongitude() <= other.getLongitudeLow() && high.getLongitude() >= other.getLongitudeHigh()));
        }
    }
}

bool Rectangle::intersects(const Rectangle& other) {
    // TODO take care of the case when the rectangles cross the
    // 180th meridian. At the moment we do not expect this predicate
    // to be used extensively, so we can safely ignore these case.
    return !(other.getLatitudeLow() > high.getLatitude() || other.getLatitudeHigh() < low.getLatitude()
             || other.getLongitudeLow() > high.getLongitude() || other.getLongitudeHigh() < low.getLongitude());
}

}// namespace NES

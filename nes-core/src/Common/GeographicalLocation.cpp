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

#include "Common/GeographicalLocation.hpp"
#include <Util/Logger.hpp>
#include <exception>

namespace NES {

GeographicalLocation::GeographicalLocation(double latitude, double longitude) {
    if (!checkValidityOfCoordinates(latitude, longitude)) {
        NES_WARNING("Trying to create node with an invalid location");
        throw CoordinatesOutOfRangeException();
    }
    this->latitude = latitude;
    this->longitude = longitude;
}

GeographicalLocation::GeographicalLocation(Coordinates coord) : GeographicalLocation(coord.lat(), coord.lng()) {}

GeographicalLocation GeographicalLocation::fromString(const std::string coordinates) {
    if (coordinates.empty()) {
        throw InvalidCoordinateFormatException();
    }
    std::stringstream ss(coordinates);
    double lat = NAN;
    ss >> lat;
    char seperator = 0;
    ss >> seperator;
    if (seperator!= ',') {
        NES_WARNING("input string is not of format \"<latitude>, <longitude>\". Node will be created as non field node");
        throw InvalidCoordinateFormatException();
    }
    double lng = NAN;
    ss >> lng;

    return {lat, lng};

}

GeographicalLocation::operator std::tuple<double, double>() {return std::make_tuple(latitude, longitude);};

GeographicalLocation::GeographicalLocation(std::tuple<double, double> coordTuple) : GeographicalLocation(std::get<0>(coordTuple), std::get<1>(coordTuple)) {}

GeographicalLocation::operator Coordinates*() {
    auto coordinates = new Coordinates;
    coordinates->set_lat(latitude);
    coordinates->set_lng(longitude);
    return coordinates;
}

bool GeographicalLocation::operator==(const GeographicalLocation& other) const {
    return this->getLatitude() == other.getLatitude() && this->getLongitude() == other.getLongitude();
};

double GeographicalLocation::getLatitude() const {
    return latitude;
}

double GeographicalLocation::getLongitude() const {
    return longitude;
}

bool GeographicalLocation::checkValidityOfCoordinates(double latitude, double longitude) {
    return !(std::abs(latitude) > 90 || std::abs(longitude) > 180);
}

}


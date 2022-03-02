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

#include <CoordinatorRPCService.pb.h>
#include <Common/GeographicalLocation.hpp>
#include <Util/Logger/Logger.hpp>
#include <Exceptions/CoordinatesOutOfRangeException.hpp>
#include <Exceptions/InvalidCoordinateFormatException.hpp>
#include <Exceptions/AccessingInvalidCoordinatesException.hpp>

namespace NES {

GeographicalLocation::GeographicalLocation() {
    latitude = 200;
    longitude = 200;
}

GeographicalLocation::GeographicalLocation(double latitude, double longitude) {
    //coordinates <200, 200> lead to the creation of an object which symbolizes an invalid location
    if (!(latitude == 200 && longitude == 200) && !checkValidityOfCoordinates(latitude, longitude)) {
        NES_WARNING("Trying to create node with an invalid location");
        throw CoordinatesOutOfRangeException();
    }
    this->latitude = latitude;
    this->longitude = longitude;
}

GeographicalLocation::GeographicalLocation(const Coordinates& coord) : GeographicalLocation(coord.lat(), coord.lng()) {}

GeographicalLocation GeographicalLocation::fromString(const std::string& coordinates) {
    if (coordinates.empty()) {
        throw InvalidCoordinateFormatException();
    }
    std::stringstream stringStream(coordinates);
    double lat;
    stringStream >> lat;
    char separator = 0;
    stringStream >> separator;
    if (separator != ',') {
        NES_WARNING("input string is not of format \"<latitude>, <longitude>\". Node will be created as non field node");
        throw InvalidCoordinateFormatException();
    }
    double lng;
    stringStream >> lng;

    return {lat, lng};
}

GeographicalLocation::operator std::tuple<double, double>() { return std::make_tuple(latitude, longitude); };

GeographicalLocation::GeographicalLocation(std::tuple<double, double> coordTuple)
    : GeographicalLocation(std::get<0>(coordTuple), std::get<1>(coordTuple)) {}

GeographicalLocation::operator Coordinates() const {
    Coordinates coordinates;
    coordinates.set_lat(latitude);
    coordinates.set_lng(longitude);
    return coordinates;
}

bool GeographicalLocation::operator==(const GeographicalLocation& other) const {
    return this->latitude == other.latitude && this->longitude == other.longitude;
};

double GeographicalLocation::getLatitude() const {
    if (!isValid()) {
        throw AccessingInvalidCoordinatesException();
    }
    return latitude;
}

double GeographicalLocation::getLongitude() const {
    if (!isValid()) {
        throw AccessingInvalidCoordinatesException();
    }
    return longitude;
}

bool GeographicalLocation::isValid() const{
    return checkValidityOfCoordinates(latitude, longitude);
}

bool GeographicalLocation::checkValidityOfCoordinates(double latitude, double longitude) {
    return !(std::abs(latitude) > 90 || std::abs(longitude) > 180);
}

}// namespace NES

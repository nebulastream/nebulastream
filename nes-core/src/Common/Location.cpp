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
#include <Exceptions/CoordinatesOutOfRangeException.hpp>
#include <Exceptions/InvalidCoordinateFormatException.hpp>
#include <Spatial/Index/Location.hpp>
#include <Util/Logger/Logger.hpp>
#include <cmath>

namespace NES::Spatial::Index::Experimental {

Location::Location() {
    latitude = std::numeric_limits<double>::quiet_NaN();
    longitude = std::numeric_limits<double>::quiet_NaN();
}

Location::Location(double latitude, double longitude) {
    //Coordinates with the value NaN lead to the creation of an object which symbolizes an invalid location
    if (!(std::isnan(latitude) && std::isnan(longitude)) && !checkValidityOfCoordinates(latitude, longitude)) {
        NES_WARNING("Trying to create node with an invalid location");
        throw CoordinatesOutOfRangeException();
    }
    this->latitude = latitude;
    this->longitude = longitude;
}

Location::Location(const Coordinates& coord) : Location(coord.lat(), coord.lng()) {}

Location Location::fromString(const std::string& coordinates) {
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

Location::operator std::tuple<double, double>() { return std::make_tuple(latitude, longitude); };

Location::Location(std::tuple<double, double> coordTuple) : Location(std::get<0>(coordTuple), std::get<1>(coordTuple)) {}

Location::operator Coordinates() const {
    Coordinates coordinates;
    coordinates.set_lat(latitude);
    coordinates.set_lng(longitude);
    return coordinates;
}

bool Location::operator==(const Location& other) const {
    //if both objects are an invalid location, consider them equal
    if (!this->isValid() && !other.isValid()) {
        return true;
    }
    return this->latitude == other.latitude && this->longitude == other.longitude;
}

double Location::getLatitude() const { return latitude; }

double Location::getLongitude() const { return longitude; }

bool Location::isValid() const { return !(std::isnan(latitude) || std::isnan(longitude)); }

std::string Location::toString() const { return std::to_string(latitude) + ", " + std::to_string(longitude); }

bool Location::checkValidityOfCoordinates(double latitude, double longitude) {
    return !(std::abs(latitude) > 90 || std::abs(longitude) > 180);
}

}// namespace NES::Spatial::Index::Experimental

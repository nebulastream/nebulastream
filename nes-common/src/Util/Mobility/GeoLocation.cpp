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
#include <Util/Mobility/GeoLocation.hpp>
#include <Util/Logger/Logger.hpp>
#include <cmath>

namespace NES::Spatial::DataTypes::Experimental {

GeoLocation::GeoLocation() {
    latitude = std::numeric_limits<double>::quiet_NaN();
    longitude = std::numeric_limits<double>::quiet_NaN();
}

GeoLocation::GeoLocation(double latitude, double longitude) {
    //Coordinates with the value NaN lead to the creation of an object which symbolizes an invalid location
    if (!(std::isnan(latitude) && std::isnan(longitude)) && !checkValidityOfCoordinates(latitude, longitude)) {
        NES_ERROR("Trying to create a location with invalid coordinates");
        throw NES::Spatial::Exception::CoordinatesOutOfRangeException();
    }
    this->latitude = latitude;
    this->longitude = longitude;
}

GeoLocation::GeoLocation(const NES::Spatial::Protobuf::GeoLocation& geoLocation)
    : GeoLocation(geoLocation.lat(), geoLocation.lng()) {}

GeoLocation GeoLocation::fromString(const std::string& coordinates) {
    if (coordinates.empty()) {
        throw NES::Spatial::Exception::CoordinatesOutOfRangeException();
    }

    std::stringstream stringStream(coordinates);
    double lat;
    stringStream >> lat;
    char separator = 0;
    stringStream >> separator;
    if (separator != ',') {
        NES_ERROR("input string is not of format \"<latitude>, <longitude>\". Node will be created as non field node");
        throw NES::Spatial::Exception::CoordinatesOutOfRangeException();
    }
    double lng;
    stringStream >> lng;

    return {lat, lng};
}

bool GeoLocation::operator==(const GeoLocation& other) const {
    //if both objects are an invalid location, consider them equal
    if (!this->isValid() && !other.isValid()) {
        return true;
    }
    return this->latitude == other.latitude && this->longitude == other.longitude;
}

double GeoLocation::getLatitude() const { return latitude; }

double GeoLocation::getLongitude() const { return longitude; }

bool GeoLocation::isValid() const { return !(std::isnan(latitude) || std::isnan(longitude)); }

std::string GeoLocation::toString() const { return std::to_string(latitude) + ", " + std::to_string(longitude); }

bool GeoLocation::checkValidityOfCoordinates(double latitude, double longitude) {
    return !(std::abs(latitude) > 90 || std::abs(longitude) > 180);
}
}// namespace NES::Spatial::DataTypes::Experimental
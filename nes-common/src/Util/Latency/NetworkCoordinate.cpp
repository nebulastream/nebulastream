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
#include <Util/Logger/Logger.hpp>
#include <Util/Latency/NetworkCoordinate.hpp>
#include <cmath>

namespace NES::Synthetic::DataTypes::Experimental {

NetworkCoordinate::NetworkCoordinate() {
    x1 = std::numeric_limits<double>::quiet_NaN();
    x2 = std::numeric_limits<double>::quiet_NaN();
}

NetworkCoordinate::NetworkCoordinate(double x1, double x2) {
    //Coordinates with the value NaN lead to the creation of an object which symbolizes an invalid location
    if (!(std::isnan(x1) && std::isnan(x2)) && !checkValidityOfCoordinates(x1, x2)) {
        NES_ERROR("Trying to create a location with invalid coordinates");
        throw NES::Spatial::Exception::CoordinatesOutOfRangeException();
    }
    this->x1 = x1;
    this->x2 = x2;
}

NetworkCoordinate::NetworkCoordinate(const NES::Synthetic::Protobuf::NetworkCoordinate& networkCoordinate)
    : NetworkCoordinate(networkCoordinate.x1(), networkCoordinate.x2()) {}

NetworkCoordinate NetworkCoordinate::fromString(const std::string& coordinates) {
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

bool NetworkCoordinate::operator==(const NetworkCoordinate& other) const {
    //if both objects are an invalid location, consider them equal
    if (!this->isValid() && !other.isValid()) {
        return true;
    }
    return this->x1 == other.x1 && this->x2 == other.x2;
}

double NetworkCoordinate::getX1() const { return x1; }

double NetworkCoordinate::getX2() const { return x2; }

bool NetworkCoordinate::isValid() const { return !(std::isnan(x1) || std::isnan(x2)); }

std::string NetworkCoordinate::toString() const { return std::to_string(x1) + ", " + std::to_string(x2); }

bool NetworkCoordinate::checkValidityOfCoordinates(double x1, double x2) {
    if (std::isnan(x1) || std::isnan(x2) || std::isinf(x1) || std::isinf(x2)) {
        return false;
    }
    return true;
}
}// namespace NES::Spatial::DataTypes::Experimental
#include "Common/GeographicalLocation.hpp"
#include <Util/Logger.hpp>
#include <exception>

namespace NES {

GeographicalLocation::GeographicalLocation(double latitude, double longitude) {
    if (!checkValidityOfCoordinates(latitude, longitude)) {
        //TODO make this less than error
        NES_ERROR("Trying to create node with an invalid location");
        //exit(EXIT_FAILURE);
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


const char * CoordinatesOutOfRangeException::what () const noexcept {
    return "Invalid latitude or longitude";
}

const char* InvalidCoordinateFormatException::what() const noexcept {
    return "The provided string is not of the format \"<lat>, <lng>\"";
}
}


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
#ifndef NES_CORE_INCLUDE_SPATIAL_INDEX_LOCATION_HPP_
#define NES_CORE_INCLUDE_SPATIAL_INDEX_LOCATION_HPP_

#include <string>

class Coordinates;

namespace NES::Spatial::Index::Experimental {

/**
 * @brief a representation of geographical location used to specify the fixed location of field nodes
 * and the changing location of mobile devices
 */
class Location {

  public:
    /**
     * @brief the default constructor which constructs an object with lat=200 and lng=200 which represents an invalid location
     */
    Location();

    /**
     * @brief constructs a Geographical location from latitude and longitude in degrees
     * @throws CoordinatesOutOfRangeException if the entered parameters do not correspond to a valid lat/long pair
     * @param latitude: geographical latitude in degrees [-90, 90]
     * @param longitude: geographical longitude in degrees [-180, 180]
     */
    Location(double latitude, double longitude);

    /**
     * @brief constructs a Geographicala location object from a Coordinates object used as members of protobuf messages
     * @throws CoordinatesOutOfRangeException if the entered parameters do not correspond to a valid lat/long pair
     * @param coord: the coordinate object
     */
    explicit Location(const Coordinates& coord);

    /**
     * @brief constructs a Geographical location from a tuple of doubles
     * @throws CoordinatesOutOfRangeException if the entered parameters do not correspond to a valid lat/long pair
     * @param coordTuple: a tuple of doubles in the format <lat, lng>
     */
    explicit Location(std::tuple<double, double> coordTuple);

    /**
     * @brief creates a tuple of doubles containing latitude and longitude of the location
     * @return a tuple in the format <lat, lng>
     */
    explicit operator std::tuple<double, double>();

    /**
     * @brief creates a protobuf Coordinate object containing the latitude and longitude allowing
     * @return a Coordinates object containing the locations latitude and longitude
     */
    explicit operator Coordinates() const;

    /**
     * @brief compares two GeographicalLocations and checks if they represent the same point on the map
     * @param other: the object to compare to
     * @return true both objects have the same latitude and longitude. false otherwise
     */
    bool operator==(const Location& other) const;

    /**
     * @brief getter for the latitude
     * @return the latitude in degrees [-90, 90]
     */
    [[nodiscard]] double getLatitude() const;

    /**
     * @brief getter for the longitude
     * @return the longitude in degrees [-180, 180]
     */
    [[nodiscard]] double getLongitude() const;

    /**
     * @brief checks if this objects represents valid coordinates or
     * invalid coordinates represented by the Coordinates 200, 200
     */
    [[nodiscard]] bool isValid() const;

    /**
     * @brief get a string representation of this object
     * @return a string in the format "latitude, longitude"
     */
    [[nodiscard]] std::string toString() const;

    /**
     * @brief Constructs a Location form a string.
     * @throws InvalidCoordinateFormatException if the input string is not of the format "<double>, <double>"
     * @throws CoordinatesOutOfRangeException if the entered parameters do not correspond to a valid lat/long pair
     * @param coordinates: string of the format "<latitude>, <longitude>"
     * @return a Location object
     */
    static Location fromString(const std::string& coordinates);

    /**
     * @brief checks if the a pair of doubles represents valid coordinates (abs(lat) < 90 and abs(lng) < 180)
     * @param latitude: the geographical latitude in degrees
     * @param longitude: the geographical longitude in degrees
     * @return true if inputs were valid geocoordinates
     */
    static bool checkValidityOfCoordinates(double latitude, double longitude);

  private:
    double latitude;
    double longitude;
};
}// namespace NES::Spatial::Index::Experimental

#endif// NES_CORE_INCLUDE_SPATIAL_INDEX_LOCATION_HPP_

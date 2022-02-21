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
#ifndef NES_NES_CORE_INCLUDE_COMMON_GEOGRAPHICALLOCATION_H_
#define NES_NES_CORE_INCLUDE_COMMON_GEOGRAPHICALLOCATION_H_
#include <CoordinatorRPCService.grpc.pb.h>
#include <Exceptions/CoordinatesOutOfRangeException.h>
#include <Exceptions/InvalidCoordinateFormatException.h>

namespace NES {

/**
 * @brief a representation of geographical location used to specify the fixed location of field nodes
 * and the changing location of mobile devices
 */
class GeographicalLocation {

  public:
    /**
     * @brief constructs a Geographical location from latitude and longitude in degrees
     * @throws CoordinatesOutOfRangeException if the entered parameters do not correspond to a valid lat/long pair
     * @param latitude: geographical latitude in degrees [-90, 90]
     * @param longitude: geographical longitude in degrees [-180, 180]
     */
    GeographicalLocation(double latitude, double longitude);

    /**
     * @brief constructs a Geographicala location object from a Coordinates object used as members of protobuf messages
     * @throws CoordinatesOutOfRangeException if the entered parameters do not correspond to a valid lat/long pair
     * @param coord: the coordinate object
     */
    GeographicalLocation(Coordinates coord);

    /**
     * @brief constructs a Geographical location from a tuple of doubles
     * @throws CoordinatesOutOfRangeException if the entered parameters do not correspond to a valid lat/long pair
     * @param coordTuple: a tuple of doubles in the format <lat, lng>
     */
    explicit GeographicalLocation(std::tuple<double, double> coordTuple);

    /**
     * @brief creates a tuple of doubles containing latitude and longitude of the location
     * @return a tuple in the format <lat, lng>
     */
    explicit operator std::tuple<double, double>();

    /**
     * @brief creates a protobuf Coordinate object containing the latitude and longitude allowing
     * @return a Coordinates object containing the locations latitude and longitude
     */
    operator Coordinates*();

    /**
     * @brief compares two GeographicalLocations and checks if they represent the same point on the map
     * @param other: the object to compare to
     * @return true both objects have the same latitude and longitude. false otherwise
     */
    bool operator==(const GeographicalLocation& other) const;

    /**
     * @brief getter for the latitude
     * @return the latitude in degrees [-90, 90]
     */
    double getLatitude() const;

    /**
     * @brief getter for the longitude
     * @return the longitude in degrees [-180, 180]
     */
    double getLongitude() const;


    /**
     * @brief Constructs a GeographicalLocation form a string.
     * @throws InvalidCoordinateFormatException if the input string is not of the format "<double>, <double>"
     * @throws CoordinatesOutOfRangeException if the entered parameters do not correspond to a valid lat/long pair
     * @param coordinates: string of the format "<latitude>, <longitude>"
     * @return a GeographicalLocation object
     */
    static GeographicalLocation fromString(const std::string coordinates);

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

}

#endif//NES_NES_CORE_INCLUDE_COMMON_GEOGRAPHICALLOCATION_H_

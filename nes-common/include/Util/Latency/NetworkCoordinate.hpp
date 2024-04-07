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
#ifndef NES_COMMON_INCLUDE_UTIL_LATENCY_NETWORKCOORDINATE_HPP_
#define NES_COMMON_INCLUDE_UTIL_LATENCY_NETWORKCOORDINATE_HPP_

#include <string>

namespace NES::Synthetic {

        namespace Protobuf {
        class NetworkCoordinate;
        }

        namespace DataTypes::Experimental {

        /**
* @brief a representation of coordinate of a node in a synthetic space used to model the Internet distances between the field nodes
* and the changing Internet distances of all devices (including the mobile devices).
*
* Currently, only the two-dimensional Euclidean network embeddings are supported, but the class will be extended to support any
* number of dimensions in either Euclidean or matrix spaces.
*/
        class NetworkCoordinate {

          public:
            /**
 * @brief the default constructor which constructs an object with x1=0 and x2=0 which represents the initial coordinates
 */
            NetworkCoordinate();

            /**
 * @brief constructs a Network Coordinate from x1 and x2 in the set of real numbers
 * @throws CoordinatesOutOfRangeException if the entered parameters do not correspond to a valid x1/x2 pair
 * @param x1: the position on the first axis in the synthetic coordinate system. Can have any value in the set of real numbers
 * @param x2: the position on the second axis in the synthetic coordinate system. Can have any value in the set of real numbers
 */
            NetworkCoordinate(double x1, double x2);

            /**
 * @brief constructs a network coordinate object from a NetworkCoordinate object used as members of protobuf messages
 * @throws CoordinatesOutOfRangeException if the entered parameters do not correspond to a valid x1/x2 pair
 * @param networkCoordinate: the coordinate object
 */
            explicit NetworkCoordinate(const NES::Synthetic::Protobuf::NetworkCoordinate& networkCoordinate);

            /**
 * @brief compares two network coordinates and checks if they represent the same point on the synthetic coordinate system
 * @param other: the object to compare to
 * @return true both objects have the same x1 and x2. false otherwise
 */
            bool operator==(const NetworkCoordinate& other) const;

            /**
 * @brief getter for x1
 * @return the x1 in real numbers
 */
            [[nodiscard]] double getX1() const;

            /**
 * @brief getter for x2
 * @return the x2 in real numbers
 */
            [[nodiscard]] double getX2() const;

            /**
 * @brief checks if this objects represents valid coordinates.
 * In the current setup of two-dimensional Euclidean embedding, all of the doubles are valid coordinates.
 */
            [[nodiscard]] bool isValid() const;

            /**
 * @brief get a string representation of this object
 * @return a string in the format "x1, x2"
 */
            [[nodiscard]] std::string toString() const;

            /**
 * @brief Constructs a Coordinate from a string.
 * @throws InvalidCoordinateFormatException if the input string is not of the format "<double>, <double>"
 * @param coordinates: string of the format "<x1>, <x2>"
 * @return a Coordinate object
 */
            static NetworkCoordinate fromString(const std::string& coordinates);

            /**
     * @brief checks if the a pair of doubles represents valid coordinates
     * @param x1: the coordinates on the x1 axis in the synthetic coordinate space
     * @param x2: the coordinates on the x1 axis in the synthetic coordinate space
     * @return true if inputs were valid network coordinates
     */
        static bool checkValidityOfCoordinates(double x1, double x2);

          private:
            double x1;
            double x2;
        };
        }// namespace DataTypes::Experimental
    }// namespace NES::Synthetic

#endif// NES_COMMON_INCLUDE_UTIL_LATENCY_NETWORKCOORDINATE_HPP_

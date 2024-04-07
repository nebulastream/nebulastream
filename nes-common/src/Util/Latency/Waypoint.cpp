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

#include <Util/Latency/Waypoint.hpp>

namespace NES::Synthetic::DataTypes::Experimental {

    Waypoint::Waypoint(NetworkCoordinate coordinate) : coordinate(coordinate), timestamp(std::nullopt){};

    Waypoint::Waypoint(NetworkCoordinate coordinate, Timestamp timestamp) : coordinate(coordinate), timestamp(timestamp) {}

    Waypoint Waypoint::invalid() { return Waypoint(NetworkCoordinate()); };

    NetworkCoordinate Waypoint::getCoordinate() const { return coordinate; }

    std::optional<Timestamp> Waypoint::getTimestamp() const { return timestamp; }
}// namespace NES::Synthetic::DataTypes::Experimental

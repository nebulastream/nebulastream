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
#ifndef NES_CORE_INCLUDE_SPATIAL_INDEX_WAYPOINT_HPP_
#define NES_CORE_INCLUDE_SPATIAL_INDEX_WAYPOINT_HPP_
#include <Util/TimeMeasurement.hpp>
#include <optional>
#include <Spatial/Index/Location.hpp>

namespace NES::Spatial::Index::Experimental {

class Waypoint {
  public:
    Waypoint(Location location);
    Waypoint(Location location, Timestamp timestamp);
    static Waypoint invalid();
    Location getLocation() const;
    std::optional<Timestamp> getTimestamp() const;

  private:
    Location location;
    std::optional<Timestamp> timestamp;
};
}

#endif//NES_CORE_INCLUDE_SPATIAL_INDEX_WAYPOINT_HPP_

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

#ifndef NES_LOCATIONSOURCECSV_HPP
#define NES_LOCATIONSOURCECSV_HPP
#include <Geolocation/LocationSource.hpp>
#include <Common/GeographicalLocation.hpp>
#include <vector>

namespace NES {
/**
 * @brief this class reads locations and timestamps from a csv file and simulates the behaviour of a geolocation interface
 * of a mobile device
 */
class LocationSourceCSV : public LocationSource {
  public:

    /**
     * @brief construct a location source that reads from a csv in the format "<latitude>, <longitued>; <offset from starttime in nanosec>
     * @param csvPath: The path of the csv file
     */
    explicit LocationSourceCSV(std::string csvPath);

    /**
     * @brief default destructor
     */
    ~LocationSourceCSV() override = default;;

    /**
     * @brief get the simulated last known location of the device. if s2 is enabled this will be an interpolated point along
     * the line between to locations from the csv. If s2 is disabled this will be the waypoint from the csv which has the
     * most recent of the timestamps lying in the past
     * @return a pair containing a goegraphical location and the time when this location was recorded
     */
    std::pair<GeographicalLocation, Timestamp> getLastKnownLocation() override;

    /**
     *
     * @return the Timestamp recorded when this object was created
     */
    [[nodiscard]] Timestamp getStarttime() const;

  private:

    Timestamp startTime;
    std::vector<std::pair<GeographicalLocation, Timestamp>> waypoints;
    std::vector<std::pair<GeographicalLocation, Timestamp>>::iterator nextWaypoint;
};
}

#endif//NES_LOCATIONSOURCECSV_HPP

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

#ifndef NES_CORE_INCLUDE_SPATIAL_MOBILITY_LOCATIONPROVIDERCSV_HPP_
#define NES_CORE_INCLUDE_SPATIAL_MOBILITY_LOCATIONPROVIDERCSV_HPP_
#include "LocationProvider.hpp"
#include <vector>

namespace NES::Spatial::Mobility::Experimental {

/**
 * @brief this class reads locations and timestamps from a csv file and simulates the behaviour of a geolocation interface
 * of a mobile device
 */
class LocationProviderCSV : public LocationProvider {
  public:
    explicit LocationProviderCSV(const std::string& csvPath);

    explicit LocationProviderCSV(const std::string& csvPath, Timestamp simulatedStartTime);

    /**
     * @brief construct a location source that reads from a csv in the format "<latitude>, <longitued>; <offset from starttime in nanosec>
     * @param csvPath: The path of the csv file
     */
    void readMovementSimulationDataFromCsv(const std::string& csvPath);

    /**
     * @brief default destructor
     */
    ~LocationProviderCSV() override = default;

    /**
     *
     * @return the Timestamp recorded when this object was created
     */
    [[nodiscard]] Timestamp getStartTime() const;

    /**
     * @brief get the simulated last known location of the device. if s2 is enabled this will be an interpolated point along
     * the line between to locations from the csv. If s2 is disabled this will be the waypoint from the csv which has the
     * most recent of the timestamps lying in the past
     * @return a pair containing a goegraphical location and the time when this location was recorded
     */
    //todo: #2951: change return type
    [[nodiscard]] Index::Experimental::WaypointPtr getCurrentWaypoint() override;

    /**
     * @brief return a vector containing all the waypoints read from the csv file
     * @return a vector of pairs of Locations and Timestamps
     */
    [[nodiscard]] const std::vector<Index::Experimental::WaypointPtr>& getWaypoints() const;

  private:
    /**
     * @brief get the waypoint at the position of the iterator
     * @param index: the iterator which marks the position in the vector of waypoints
     * @return the waypoint
     */
    Index::Experimental::WaypointPtr getWaypointAt(size_t index);

    Timestamp startTime;
    std::vector<Index::Experimental::WaypointPtr> waypoints;
    size_t nextWaypoint;
};
}// namespace NES::Spatial::Mobility::Experimental

#endif// NES_CORE_INCLUDE_SPATIAL_MOBILITY_LOCATIONPROVIDERCSV_HPP_

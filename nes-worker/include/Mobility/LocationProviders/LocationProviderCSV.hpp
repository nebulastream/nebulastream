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

#ifndef NES_CORE_INCLUDE_MOBILITY_LOCATIONPROVIDERS_LOCATIONPROVIDERCSV_HPP_
#define NES_CORE_INCLUDE_MOBILITY_LOCATIONPROVIDERS_LOCATIONPROVIDERCSV_HPP_

#include <Mobility/LocationProviders/LocationProvider.hpp>
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
     *
     * @return the Timestamp recorded when this object was created
     */
    [[nodiscard]] Timestamp getStartTime() const;

    /**
     * @brief get the simulated current location of the device which is the waypoint read from csv which has the
     * most recent of the timestamps lying in the past
     * @return a pair containing a goegraphical location and the time when this location was recorded
     */
    //todo: #2951: change return type
    [[nodiscard]] DataTypes::Experimental::Waypoint getCurrentWaypoint() override;

  private:
    /**
     * @brief reads waypoints representing simulated device locations from a csv in the format
     * "<latitude>, <longitued>; <offset from starttime in nanosec>".
     * getCurrentWaypoint() will return a waypoints from this list.
     */
    void loadMovementSimulationDataFromCsv();

    /**
     * @brief get the waypoint at the position of the iterator
     * @param index: the iterator which marks the position in the vector of waypoints
     * @return the waypoint
     */
    DataTypes::Experimental::Waypoint getWaypointAt(size_t index);

    Timestamp startTime;
    std::vector<DataTypes::Experimental::Waypoint> waypoints;
    size_t nextWaypointIndex;
    std::string csvPath;
};
}// namespace NES::Spatial::Mobility::Experimental

#endif// NES_CORE_INCLUDE_MOBILITY_LOCATIONPROVIDERS_LOCATIONPROVIDERCSV_HPP_

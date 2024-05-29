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

#ifndef NES_WORKER_INCLUDE_LATENCY_NETWORKCOORDINATEPROVIDERS_NETWORKCOORDINATEPROVIDERCSV_HPP_
#define NES_WORKER_INCLUDE_LATENCY_NETWORKCOORDINATEPROVIDERS_NETWORKCOORDINATEPROVIDERCSV_HPP_

#include <Latency/NetworkCoordinateProviders/NetworkCoordinateProvider.hpp>
#include <vector>

namespace NES::Synthetic::Latency::Experimental {

    /**
    * @brief this class reads network coordinates and timestamps from a csv file and simulates the behaviour of a network coordinate interface
    * of a worker
    */
class NetworkCoordinateProviderCSV : public NetworkCoordinateProvider {
  public:
    explicit NetworkCoordinateProviderCSV(const std::string& csvPath);

    explicit NetworkCoordinateProviderCSV(const std::string& csvPath, Timestamp simulatedStartTime);

    /**
     *
     * @return the Timestamp recorded when this object was created
     */
    [[nodiscard]] Timestamp getStartTime() const;

    /**
     * @brief get the simulated current network coordinate of the device which is the waypoint read from csv which has the
     * most recent of the timestamps lying in the past
     * @return a pair containing a network coordinates and the time when this network coordinate was recorded
     */
    [[nodiscard]] DataTypes::Experimental::Waypoint getCurrentWaypoint() override;

  private:
    /**
     * @brief reads waypoints representing simulated network coordinates from a csv in the format
     * "<x1>, <x2>; <offset from starttime in nanosec>".
     * getCurrentWaypoint() will return a waypoints from this list.
     */
    void loadNetworkCoordinateSimulationDataFromCsv();

    /**
     * @brief get the waypoint at the position of the iterator
     * @param index: the iterator which marks the position in the vector of waypoints
     * @return the waypoint
     */
    DataTypes::Experimental::Waypoint getWaypointAt(size_t index);

    Timestamp startTime;
    std::vector<DataTypes::Experimental::Waypoint> waypoints;
    size_t nextWaypointIndex = 0;
    std::string csvPath;
};
}// namespace NES::Synthetic::Latency::Experimental

#endif// NES_WORKER_INCLUDE_LATENCY_NETWORKCOORDINATEPROVIDERS_NETWORKCOORDINATEPROVIDERCSV_HPP_

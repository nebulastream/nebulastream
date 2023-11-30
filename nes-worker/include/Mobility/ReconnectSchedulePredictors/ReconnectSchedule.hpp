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
#ifndef NES_WORKER_INCLUDE_MOBILITY_RECONNECTSCHEDULEPREDICTORS_RECONNECTSCHEDULE_HPP_
#define NES_WORKER_INCLUDE_MOBILITY_RECONNECTSCHEDULEPREDICTORS_RECONNECTSCHEDULE_HPP_

#include <Util/Mobility/GeoLocation.hpp>
#include <Util/Mobility/ReconnectPoint.hpp>
#include <Util/TimeMeasurement.hpp>
#include <memory>
#include <vector>

namespace NES::Spatial::Mobility::Experimental {

/**
 * @brief contains the predicted reconnect points along the trajectory of this worker.
 * Note: As the current location of this worker changes, the ReconnectSchedulePredictor can potentially re-compute all previous predictions.
 */
class ReconnectSchedule {
  public:
    /**
     * Constructor
     * @param reconnectVector a vector containing times, locations and new parent ids for the expected reconnects
     */
    explicit ReconnectSchedule(std::vector<ReconnectPoint> reconnectVector);
    /**
     * @brief getter function for the vector containing the scheduled reconnects
     * @return a vector containing reconnect points consisting of expected next parent id, estimated reconnect location, estimated reconnect time
     */
    [[nodiscard]] const std::vector<ReconnectPoint>& getReconnectVector() const;

    /**
     * @brief removes the upcoming scheduled reconnect point from the front of the list
     * @return true on success
     */
    bool removeNextReconnect();

    /**
     * @brief get a reconnect schedule object which does not contain any values to represent that no prediction exists
     * @return a reconnect schedule with all its members set to nullptr
     */
    static ReconnectSchedule Empty();

  private:
    std::vector<ReconnectPoint> reconnectVector;
};
}// namespace NES::Spatial::Mobility::Experimental

#endif  // NES_WORKER_INCLUDE_MOBILITY_RECONNECTSCHEDULEPREDICTORS_RECONNECTSCHEDULE_HPP_

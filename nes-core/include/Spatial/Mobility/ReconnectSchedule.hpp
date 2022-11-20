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
#ifndef NES_CORE_INCLUDE_SPATIAL_MOBILITY_RECONNECTSCHEDULE_HPP_
#define NES_CORE_INCLUDE_SPATIAL_MOBILITY_RECONNECTSCHEDULE_HPP_
#include "Util/TimeMeasurement.hpp"
#include <memory>
#include <vector>
namespace NES::Spatial::Index::Experimental {
class Location;
using LocationPtr = std::shared_ptr<Location>;
}// namespace NES::Spatial::Index::Experimental

namespace NES::Spatial::Mobility::Experimental {
struct ReconnectPrediction;
struct ReconnectPoint;

/**
 * @brief contains a prediction about a mobile devices trajectory and the reconnects as they are expected to happen
 */
class ReconnectSchedule {
  public:
    /**
     * Constructor
     * @param pathBeginning the starting point of the predicted path
     * @param pathEnd the endpoint of the predicted path
     * @param lastIndexUpdatePosition the device position at the moment when the info in the local index of field node positions
     * was downloaded from the coordinator
     * @param reconnectVector a vector containing times, locations and new parent ids for the expected reconnects
     */
    ReconnectSchedule(uint64_t currentParentId,
                      Index::Experimental::LocationPtr pathBeginning,
                      Index::Experimental::LocationPtr pathEnd,
                      Index::Experimental::LocationPtr lastIndexUpdatePosition,
                      std::shared_ptr<std::vector<std::shared_ptr<ReconnectPoint>>> reconnectVector);

    /**
     * @brief getter function for the start location of the current predicted path
     * @return a smart pointer to a location object with the coordinates of the path beginning
     */
    [[nodiscard]] Index::Experimental::LocationPtr getPathStart() const;

    /**
     * @brief getter function for the end location of the current predicted path
     * @return a smart pointer to a location object with the coordinates of the path end
     */
    [[nodiscard]] Index::Experimental::LocationPtr getPathEnd() const;

    /**
     * @brief getter function for the location at which the device was located when the last download of field node location data
     * to the local spatial index happened
     * @return a smart pointer to a location object with the coordinates of the device position of the time of updating
     */
    [[nodiscard]] Index::Experimental::LocationPtr getLastIndexUpdatePosition() const;

    /**
     * @brief getter function for the vector containing the scheduled reconnects
     * @return a vector containing tuples consisting of expected next parent id, estimated reconnect location, estimated reconnect time
     */
    [[nodiscard]] std::shared_ptr<std::vector<std::shared_ptr<ReconnectPoint>>> getReconnectVector() const;

    [[nodiscard]] uint64_t getCurrentParentId();

    /**
     * @brief get a reconnect schedule object which does not contain any values to represent that no prediction exists
     * @return a reconnect schedule with all its members set to nullptr
     */
    static ReconnectSchedule Empty();

  private:
    uint64_t currentParentId;
    Index::Experimental::LocationPtr pathStart;
    Index::Experimental::LocationPtr pathEnd;
    Index::Experimental::LocationPtr lastIndexUpdatePosition;
    std::shared_ptr<std::vector<std::shared_ptr<NES::Spatial::Mobility::Experimental::ReconnectPoint>>> reconnectVector;
};
}// namespace NES::Spatial::Mobility::Experimental

#endif// NES_CORE_INCLUDE_SPATIAL_MOBILITY_RECONNECTSCHEDULE_HPP_

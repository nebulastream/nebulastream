/*
    Copyright (C) 2021 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_INCLUDE_UTIL_TIMER_HPP_
#define NES_INCLUDE_UTIL_TIMER_HPP_

#include <Util/Logger.hpp>

#include <chrono>
#include <vector>
#include <string>
#include <utility>

using namespace std::chrono;

namespace NES {



/**
 * @brief Util class to measure the time of NES components and sub-components
 * using snapshots
 */
template<typename timeUnit = nanoseconds> class Timer {
  public:
    struct snapshot {
        std::string name;
        timeUnit duration;
    };

    Timer(std::string componentName) : componentName(componentName){};

    /**
     * @brief starts the timer or resumes it after a pause
     */
    void start();

    /**
     * @brief pauses the timer
     */
    void pause();

    /**
     * @brief Saves current duration as a snapshot. Usefull for
     * measuring the time of sub-components.
     * @note The duration is the time from the last snapshot
     * till now if saveSnapshot was called earlier. Otherwise
     * it is the time from the start call till now.
     * @param snapshotName the of the snapshot
     */
    void snapshot(std::string snapshotName);

    /**
     * @brief includes snapshots of another timer
     * instance into this instance and adds overall
     * runtime.
     * @param Timer to be merged with this instance
     */
    void merge(Timer timer);

    /**
     * @brief retruns the currently saved snapshots
     */
    std::vector<struct snapshot> getSnapshots() const { return snapshots; };

    /**
     * @brief returns the current runtime
     * @note will return zero if timer is not paused
     */
     unsigned int getRuntime() const;

    /**
    * @brief returns the component name to measure
    */
     std::string getComponentName() const { return componentName;};

    /**
     * @brief overwrites insert string operator
     */
     friend std::ostream& operator<< (std::ostream& Str, const Timer<timeUnit>& t);

  private:
    /**
     * @brief component name to measure
     */
    std::string componentName;

    /**
     * @brief overall measured runtime
     */
    timeUnit runtime{0};

    /**
     * @brief already passed duration after timer pause
     */
    timeUnit pausedDuration{0};

    /**
     * @brief vector to store snapshots
     */
    std::vector<struct snapshot> snapshots;

    /**
     * @brief timepoint to store start point
     */
    std::chrono::time_point<high_resolution_clock> start_p;

    /**
      * @brief timepoint to store start point
      */
    std::chrono::time_point<high_resolution_clock> stop_p;

    /**
     * @brief helper parameters
     */
    bool running{false};
};
}
#endif//NES_INCLUDE_UTIL_TIMER_HPP_

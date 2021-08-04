/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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
#include <string>
#include <utility>
#include <vector>

namespace NES {
/**
 * @brief Util class to measure the time of NES components and sub-components
 * using snapshots
 */
template<typename timeUnit = std::chrono::nanoseconds>
class Timer {
  public:
    struct snapshot {
        unsigned int getRuntime() { return runtime.count(); }
        std::string name;
        timeUnit runtime;
        std::vector<snapshot> children;
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
     * @brief saves current runtime as a snapshot. Usefull for
     * measuring the time of sub-components.
     * @note The runtime is the time from the last taken snapshot
     * till now if saveSnapshot was called earlier. Otherwise
     * it is the time from the start call till now.
     * @param snapshotName the of the snapshot
     */
    void snapshot(std::string snapshotName);

    /**
     * @brief includes snapshots of another timer
     * instance into this instance and adds overall
     * runtime.
     * @param timer to be merged with
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
    std::string getComponentName() const { return componentName; };

    /**
     * @brief overwrites insert string operator
     */
    friend std::ostream& operator<<(std::ostream& str, const Timer& t) {
        str << "overall runtime: " << t.getRuntime() << '\n';
        for (auto& s : t.getSnapshots()) {
            str << Timer<timeUnit>::printHelper(std::string(), s);
        }
        return str;
    };

    /**
     * @brief helper function for insert string operator
     */
    static std::string printHelper(std::string str, struct snapshot s);

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
     * @brief already passed runtime after timer pause
     */
    timeUnit pausedDuration{0};

    /**
     * @brief vector to store snapshots
     */
    std::vector<struct snapshot> snapshots;

    /**
     * @brief timepoint to store start point
     */
    std::chrono::time_point<std::chrono::high_resolution_clock> start_p;

    /**
      * @brief timepoint to store start point
      */
    std::chrono::time_point<std::chrono::high_resolution_clock> stop_p;

    /**
     * @brief helper parameters
     */
    bool running{false};
};
}// namespace NES
#endif//NES_INCLUDE_UTIL_TIMER_HPP_
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

#pragma once

#include <chrono>
#include <string>
#include <utility>
#include <vector>
#include <Util/Logger/Logger.hpp>

namespace NES
{

/**
 * @brief Util class to measure the time of NES components and sub-components
 * using snapshots
 */
template <
    typename TimeUnit = std::chrono::nanoseconds,
    typename PrintTimeUnit = std::milli,
    typename PrintTimePrecision = double,
    typename ClockType = std::chrono::high_resolution_clock>
class Timer
{
public:
    class Snapshot
    {
    public:
        Snapshot(std::string name, TimeUnit runtime, std::vector<Snapshot> children)
            : name(std::move(name)), runtime(runtime), children(children) { };

        int64_t getRuntime() { return runtime.count(); }

        PrintTimePrecision getPrintTime()
        {
            return std::chrono::duration_cast<std::chrono::duration<PrintTimePrecision, PrintTimeUnit>>(runtime).count();
        }

        std::string name;
        TimeUnit runtime;
        std::vector<Snapshot> children;
    };

    Timer(std::string componentName) : componentName(componentName) { };

    /**
     * @brief starts the timer or resumes it after a pause
     */
    void start()
    {
        if (running)
        {
            NES_DEBUG("Timer: Trying to start an already running timer so will skip this operation");
        }
        else
        {
            running = true;
            start_p = ClockType::now();
        }
    };

    /**
     * @brief pauses the timer
     */
    void pause()
    {
        if (!running)
        {
            NES_DEBUG("Timer: Trying to stop an already stopped timer so will skip this operation");
        }
        else
        {
            running = false;
            stop_p = ClockType::now();
            auto duration = std::chrono::duration_cast<TimeUnit>(stop_p - start_p);

            pausedDuration += duration;
            runtime += duration;
        }
    };

    /**
     * @brief Creates a fully qualified name (how it is stored)
     * @param snapshotName
     * @return Fully qualified name as string
     */
    std::string createFullyQualifiedSnapShotName(const std::string& snapshotName) { return componentName + '_' + snapshotName; }

    /**
     * @brief saves current runtime as a snapshot. Useful for
     * measuring the time of sub-components.
     * @note The runtime is the time from the last taken snapshot
     * till now if saveSnapshot was called earlier. Otherwise
     * it is the time from the start call till now.
     * @param snapshotName the of the snapshot
     */
    void snapshot(std::string snapshotName)
    {
        if (!running)
        {
            NES_DEBUG("Timer: Trying to take a snapshot of an non-running timer so will skip this operation");
        }
        else
        {
            running = true;
            stop_p = ClockType::now();
            auto duration = std::chrono::duration_cast<TimeUnit>(stop_p - start_p);

            runtime += duration;
            snapshots.emplace_back(Snapshot(createFullyQualifiedSnapShotName(snapshotName), duration, std::vector<Snapshot>()));

            start_p = ClockType::now();
        }
    };

    /**
     * @brief includes snapshots of another timer
     * instance into this instance and adds overall
     * runtime.
     * @param timer to be merged with
     */
    void merge(Timer timer)
    {
        if (running)
        {
            NES_DEBUG("Timer: Trying to merge while timer is running so will skip this operation");
        }
        else
        {
            this->runtime += timer.runtime;
            snapshots.emplace_back(Snapshot(componentName + '_' + timer.getComponentName(), timer.runtime, timer.getSnapshots()));
        }
    };

    /**
     * @brief returns the currently saved snapshots
     * @return reference to the saved snapshots
     */
    const std::vector<Snapshot>& getSnapshots() const { return snapshots; };

    /**
     * @brief Returns the runtime of the snapshot with the snapShotName
     * @param snapShotName
     * @return Runtime
     */
    int64_t getRuntimeFromSnapshot(const std::string& snapShotName)
    {
        auto it
            = std::find_if(snapshots.begin(), snapshots.end(), [&](const Snapshot& snapshot) { return (snapshot.name == snapShotName); });
        if (it != snapshots.end())
        {
            return it->getRuntime();
        }
        else
        {
            return -1;
        }
    }

    /**
     * @brief returns the current runtime
     * @note will return zero if timer is not paused
     */
    int64_t getRuntime() const
    {
        if (!running)
        {
            return runtime.count();
        }
        else
        {
            NES_DEBUG("Timer: Trying get runtime while timer is running so will return zero");
            return 0;
        }
    };

    /**
    * @brief returns the component name to measure
    */
    const std::string& getComponentName() const { return componentName; };

    /**
     * @brief overwrites insert string operator
     */
    friend std::ostream& operator<<(std::ostream& str, const Timer& t)
    {
        str << "overall runtime: " << t.getPrintTime() << getTimeUnitString();
        for (auto& s : t.getSnapshots())
        {
            str << Timer<TimeUnit, PrintTimeUnit, PrintTimePrecision>::printHelper(std::string(), s);
        }
        return str;
    };

    /**
     * @brief return timer runtime converted ConvertUnit with ConvertPrecision e.g. for printing purposes.
     */
    template <typename ConvertUnit = PrintTimeUnit, typename ConvertPrecision = PrintTimePrecision>
    ConvertPrecision getPrintTime() const
    {
        auto printDuration = std::chrono::duration_cast<std::chrono::duration<ConvertPrecision, ConvertUnit>>(runtime);
        return printDuration.count();
    }

    /**
     * @brief helper function for insert string operator
     * recursively goes through the (probably) nested snapshots and prints them
     */
    static std::string printHelper(std::string str, Snapshot s)
    {
        std::ostringstream ostr;
        ostr << str << '\n' << s.name + ":\t" << s.getPrintTime() << getTimeUnitString();

        for (auto& c : s.children)
        {
            ostr << printHelper(str, c);
        }
        return ostr.str();
    }

    /**
     * @brief helper function to return a time unit literal string based on PrintTimeUnit
     */
    template <typename ConvertUnit = PrintTimeUnit>
    static std::string getTimeUnitString()
    {
        if constexpr (std::is_same_v<ConvertUnit, std::nano>)
        {
            return " ns";
        }
        else if constexpr (std::is_same_v<ConvertUnit, std::micro>)
        {
            return " µs";
        }
        else if constexpr (std::is_same_v<ConvertUnit, std::milli>)
        {
            return " ms";
        }
        else if constexpr (std::is_same_v<ConvertUnit, std::ratio<1>>)
        {
            return " s";
        }
        else
        {
            return " unknown time units";
        }
    }

private:
    /**
     * @brief component name to measure
     */
    std::string componentName;

    /**
     * @brief overall measured runtime
     */
    TimeUnit runtime{0};

    /**
     * @brief already passed runtime after timer pause
     */
    TimeUnit pausedDuration{0};

    /**
     * @brief vector to store snapshots
     */
    std::vector<Snapshot> snapshots;

    /**
     * @brief timepoint to store start point
     */
    std::chrono::time_point<ClockType> start_p;

    /**
      * @brief timepoint to store start point
      */
    std::chrono::time_point<ClockType> stop_p;

    /**
     * @brief helper parameters
     */
    bool running{false};
};
}

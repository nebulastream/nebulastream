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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_PROFILER_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_PROFILER_HPP_

#include <asm/unistd.h>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <linux/perf_event.h>
#include <map>
#include <string>
#include <sys/ioctl.h>
#include <unistd.h>
#include <vector>
namespace NES::Runtime::Execution {
/**
* @brief Profiler that enables performance monitoring by calling perf_event_open().
*/
class Profiler {
  public:
    /**
     * @brief Constructor for the profiler.
     */
    Profiler();

    /**
     * @brief Constructor for the profiler.
     * @param events vector of events that should be profiled.
     */
    Profiler(std::vector<perf_hw_id> events);

    /**
     * @brief Starts profiling the code.
     */
    void startProfiling() const;

    /**
     * @brief Ends profiling the code.
     */
    void stopProfiling();

    /**
     * @brief Add an event for the profiler to measure.
     * @param event e.g., PERF_COUNT_HW_CACHE_MISSES.
     * @return id of event.
     */
    uint64_t addEvent(perf_hw_id event);

    /**
     * @brief Get the id that the profiler assigned the event to.
     * @param event e.g., PERF_COUNT_HW_CACHE_MISSES.
     * @return id of event.
     */
    uint64_t getEventId(perf_hw_id event);

    /**
     * @brief Get the count of the event, e.g., number of branch misses, number of cache misses.
     * @param eventId id of the event that is returned by getEventId(event).
     * @return the count that the profiler measured of the event.
     */
    uint64_t getCount(uint64_t eventId) const;

    /**
     * @brief Writes the count of all events to an output file.
     * @param fileName path to the file.
     */
    void writeToOutputFile(const char *fileName);
    ~Profiler();

  private:
    static long perf_event_open(struct perf_event_attr *hw_event, pid_t pid, int cpu, int group_fd, unsigned long flags);

    int fileDescriptor;
    struct perf_event_attr pe{};
    std::map<perf_hw_id,uint64_t> eventToIdMap;
    char buf[4096]{};
    struct read_format* rfPtr = reinterpret_cast<read_format*>(buf);
};

struct read_format {
    uint64_t nr;
    struct {
        uint64_t value;
        uint64_t id;
    } values[1];
};

}// namespace NES::Runtime::Execution
#endif// NES_RUNTIME_INCLUDE_EXECUTION_PROFILER_HPP_
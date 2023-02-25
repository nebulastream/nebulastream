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
* @brief Profiler that collects branch misses.
*/
class Profiler {
  public:
    /**
     * @brief Constructor for the profiler.
     *
     */
    Profiler(std::vector<perf_hw_id> events);

    /**
     * @brief Starts profiling of the code.
     */
    void startProfiling() const;

    /**
     * @brief Ends profiling the code.
     */
    void stopProfiling();

    /**
     * @brief Get the id that the profiler assigned the event to.
     * @param event
     * @return id of event.
     */
    uint64_t getEventId(perf_hw_id event) ;

    /**
     * @brief Get the number of branch misses.
     * @return number of branch misses.
     */
    uint64_t getCount(uint64_t eventId) const;

    /**
     * @brief Writes the number of branch misses to an output file.
     * @return name of the output file.
     */
    const char* writeToOutputFile();
    ~Profiler();

  private:
    static long perf_event_open(struct perf_event_attr *hw_event, pid_t pid, int cpu, int group_fd, unsigned long flags);

    struct perf_event_attr pe;
    size_t numberOfEvents;
    std::vector<uint64_t> eventIds;
    std::map<perf_hw_id,uint64_t> eventToIdMap;
    int fileDescriptor;
    //uint64_t count;
    char buf[4096];
    struct read_format* rfPtr = reinterpret_cast<read_format*>(buf);
    FILE* outputFile;
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
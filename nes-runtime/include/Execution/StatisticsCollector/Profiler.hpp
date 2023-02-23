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
#include <linux/perf_event.h>
#include <string>
#include <sys/ioctl.h>
#include <unistd.h>

namespace NES::Runtime::Execution {

class Profiler {
  public:
    Profiler();
    long perf_event_open(struct perf_event_attr *hw_event, pid_t pid, int cpu, int group_fd, unsigned long flags);
    void startProfiling();
    void stopProfiling();
    uint64_t getCount();
    const char* writeToOutputFile();
    ~Profiler();

  private:
    struct perf_event_attr pe;
    int fileDescriptor;
    uint64_t count;
    FILE* outputFile;
};

}// namespace NES::Runtime::Execution
#endif// NES_RUNTIME_INCLUDE_EXECUTION_PROFILER_HPP_
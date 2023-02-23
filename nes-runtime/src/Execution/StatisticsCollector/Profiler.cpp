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

#include "Execution/StatisticsCollector/Profiler.hpp"

namespace NES::Runtime::Execution {

long Profiler::perf_event_open(struct perf_event_attr *hw_event, pid_t pid, int cpu, int group_fd, unsigned long flags) {
    int64_t ret; // try int
    ret = syscall(__NR_perf_event_open, hw_event, pid, cpu, group_fd, flags);
    return ret;
}

void Profiler::startProfiling(){
    ioctl(fileDescriptor, PERF_EVENT_IOC_RESET, 0);
    ioctl(fileDescriptor, PERF_EVENT_IOC_ENABLE, 0);
}

void Profiler::stopProfiling(){
    ioctl(fileDescriptor, PERF_EVENT_IOC_DISABLE, 0);
    read(fileDescriptor, &count, sizeof(uint64_t));
}



Profiler::Profiler (){
    memset(&pe, 0, sizeof(pe));
    pe.type = PERF_TYPE_HARDWARE;
    pe.config = PERF_COUNT_HW_BRANCH_MISSES;
    pe.size = sizeof(pe);
    pe.disabled = 1;

    // for user events only
    pe.exclude_kernel = 1;
    pe.exclude_hv = 1;

    fileDescriptor = perf_event_open(&pe, 0, -1, -1, 0);
    if (fileDescriptor == -1) {
        fprintf(stderr, "Error opening perf event: %m\n");
        exit(1);
    }
}

uint64_t Profiler::getCount() {
    return count;
}

const char* Profiler::writeToOutputFile() {
    const char *fileName = "profilerOut.txt";

    outputFile = fopen(fileName, "a+");

    if (outputFile != NULL) {
        fprintf(outputFile, "Branch misses: %lu\n", count);
        fclose(outputFile);
        return fileName;
    }

    return nullptr;
}

Profiler::~Profiler() {
    close(fileDescriptor);
}





}// namespace NES::Runtime::Execution
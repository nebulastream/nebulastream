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
#include "Util/Logger/Logger.hpp"

namespace NES::Runtime::Execution {

Profiler::Profiler (){
    fileDescriptor = -1;
};

Profiler::Profiler (std::vector<perf_hw_id> events){
    fileDescriptor = -1;

    for (auto & event : events) {
        memset(&pe, 0, sizeof(pe));
        pe.type = PERF_TYPE_HARDWARE;
        pe.config = event; // specify event e.g., branch misses or cache misses
        pe.read_format = PERF_FORMAT_GROUP | PERF_FORMAT_ID; // for multiple events
        pe.size = sizeof(pe);
        pe.disabled = 1;

        // only profile on user level
        pe.exclude_kernel = 1;
        pe.exclude_hv = 1;

        auto eventFileDescriptor = perf_event_open(&pe, 0, -1, fileDescriptor, 0);
        if (eventFileDescriptor == -1) {
            NES_THROW_RUNTIME_ERROR("Error opening perf event");
        }

        eventToIdMap[event] = 0;
        ioctl(eventFileDescriptor, PERF_EVENT_IOC_ID, &eventToIdMap[event]);

        if (fileDescriptor == -1){
            // use one filedescriptor for a group of events
            fileDescriptor = eventFileDescriptor;
        }
    }

}

void Profiler::startProfiling() const {
    ioctl(fileDescriptor, PERF_EVENT_IOC_RESET, PERF_IOC_FLAG_GROUP);
    ioctl(fileDescriptor, PERF_EVENT_IOC_ENABLE, PERF_IOC_FLAG_GROUP);
}

void Profiler::stopProfiling() {
    ioctl(fileDescriptor, PERF_EVENT_IOC_DISABLE, PERF_IOC_FLAG_GROUP);
    read(fileDescriptor, buf, sizeof(buf));
}

uint64_t Profiler::addEvent(perf_hw_id event){
    memset(&pe, 0, sizeof(pe));
    pe.type = PERF_TYPE_HARDWARE; // has to be changed for software events
    pe.config = event; // specify event e.g., branch misses or cache misses
    pe.read_format = PERF_FORMAT_GROUP | PERF_FORMAT_ID; // for multiple events
    pe.size = sizeof(pe);
    pe.disabled = 1;

    // only profile on user level
    pe.exclude_kernel = 1;
    pe.exclude_hv = 1;

    auto eventFileDescriptor = perf_event_open(&pe, 0, -1, fileDescriptor, 0);
    if (eventFileDescriptor == -1) {
        NES_THROW_RUNTIME_ERROR("Error opening perf event");
    }

    eventToIdMap[event] = 0;
    ioctl(eventFileDescriptor, PERF_EVENT_IOC_ID, &eventToIdMap[event]);

    if (fileDescriptor == -1){
        // use one filedescriptor for a group of events
        fileDescriptor = eventFileDescriptor;
    }

    return eventToIdMap[event];
}


uint64_t Profiler::getEventId(perf_hw_id event) {
    if (eventToIdMap.contains(event)){
        return eventToIdMap[event];
    } else {
        NES_THROW_RUNTIME_ERROR("Event not registered");
    }

}

uint64_t Profiler::getCount(uint64_t eventId) const {
    for (uint64_t i = 0; i < rfPtr->nr; i++){
        if (rfPtr->values[i].id == eventId) {
            return rfPtr->values[i].value;
        }
    }
    return 0;
}

void Profiler::writeToOutputFile(const char *fileName) {
    FILE* outputFile = fopen(fileName, "a+");

    if (outputFile != nullptr) {
        for (uint64_t i = 0; i < rfPtr->nr; i++){
            fprintf(outputFile, "%lu\t%lu\n", rfPtr->values[i].id, rfPtr->values[i].value);
        }
        fclose(outputFile);
    } else {
        NES_THROW_RUNTIME_ERROR("Error opening file");
    }
}

Profiler::~Profiler() {
    close(fileDescriptor);
}

long Profiler::perf_event_open(struct perf_event_attr *hw_event, pid_t pid, int cpu, int group_fd, unsigned long flags) {
    int64_t ret;
    ret = syscall(__NR_perf_event_open, hw_event, pid, cpu, group_fd, flags);
    return ret;
}

}// namespace NES::Runtime::Execution
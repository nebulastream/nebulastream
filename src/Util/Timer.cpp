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

#include <Util/Timer.hpp>

using namespace std::chrono;

namespace NES {

template<class timeUnit>
void Timer<timeUnit>::start(){
    if (running){
        NES_DEBUG("Timer: Trying to start an already running timer so will skip this operation");
    } else {
        running = true;
        start_p = high_resolution_clock::now();
    }
};

template<class timeUnit>
void Timer<timeUnit>::pause(){
    if (!running){
        NES_DEBUG("Timer: Trying to stop an already stopped timer so will skip this operation");
    } else {
        running = false;

        stop_p = high_resolution_clock::now();
        auto duration = duration_cast<timeUnit>(stop_p - start_p);

        pausedDuration += duration;
        runtime += duration;
    }
};

template<class timeUnit>
void Timer<timeUnit>::snapshot(std::string snapshotName){
    if (!running){
        NES_DEBUG("Timer: Trying to take a snapshot of an non-running timer so will skip this operation");
    } else {
        running = true;

        stop_p = high_resolution_clock::now();
        auto duration = duration_cast<timeUnit>(stop_p - start_p);

        runtime += duration;

        const std::pair<std::string, timeUnit> p =
            std::make_pair(componentName + "_" + snapshotName, duration);
        snapshots.push_back(p);

        start_p = high_resolution_clock::now();
    }
};

template<class timeUnit>
void Timer<timeUnit>::merge(Timer timer) {
    if (running) {
        NES_DEBUG("Timer: Trying to merge while timer is running so will skip this operation");
    } else {
        this->runtime += timer.runtime;
        for (auto& s : timer.getSnapshots())
            snapshots.push_back(s);
    }
};

template<class timeUnit>
unsigned int Timer<timeUnit>::getRuntime() const {
    if (!running) {
        return runtime.count();
    } else {
        NES_DEBUG("Timer: Trying get runtime while timer is running so will return zero");
        return 0;
    }
};

template<class timeUnit>
std::ostream& operator<<(std::ostream& Str, const Timer<timeUnit>& t) {
    Str << "overall runtime: " << t.getRuntime() << "\n";
    for (auto& s : t.getSnapshots()) {
        Str << s.first + ": " << s.second.count() << " ns" << "\n";
    }
    return Str;
};

template void Timer<nanoseconds>::start();
template void Timer<nanoseconds>::pause();
template void Timer<nanoseconds>::snapshot(std::string snapshotName);
template void Timer<nanoseconds>::merge(Timer timer);
template unsigned int Timer<nanoseconds>::getRuntime() const;
template std::ostream& operator<<(std::ostream& Str, const Timer<nanoseconds>& t);

template void Timer<milliseconds>::start();
template void Timer<milliseconds>::pause();
template void Timer<milliseconds>::snapshot(std::string snapshotName);
template void Timer<milliseconds>::merge(Timer timer);
template unsigned int Timer<milliseconds>::getRuntime() const;
template std::ostream& operator<<(std::ostream& Str, const Timer<milliseconds>& t);

template void Timer<seconds>::start();
template void Timer<seconds>::pause();
template void Timer<seconds>::snapshot(std::string snapshotName);
template void Timer<seconds>::merge(Timer timer);
template unsigned int Timer<seconds>::getRuntime() const;
template std::ostream& operator<<(std::ostream& Str, const Timer<seconds>& t);

}// namespace NES
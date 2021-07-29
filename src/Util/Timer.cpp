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

void Timer::start(){
    if (running){
        NES_DEBUG("Timer: Trying to start an already running timer so will skip this operation");
    } else {
        running = true;
        start_p = high_resolution_clock::now();
    }
};

void Timer::pause(){
    if (!running){
        NES_DEBUG("Timer: Trying to stop an already stopped timer so will skip this operation");
    } else {
        running = false;

        stop_p = high_resolution_clock::now();
        auto duration = duration_cast<nanoseconds>(stop_p - start_p);

        pausedDuration += duration;
        runtime += duration;
    }
};

void Timer::snapshot(std::string snapshotName){
    if (!running){
        NES_DEBUG("Timer: Trying to take a snapshot of an non-running timer so will skip this operation");
    } else {
        running = true;

        stop_p = high_resolution_clock::now();
        auto duration = duration_cast<nanoseconds>(stop_p - start_p);

        runtime += duration;

        const std::pair<std::string, std::chrono::nanoseconds> p =
            std::make_pair(componentName + "_" + snapshotName, duration);
        snapshots.push_back(p);

        start_p = high_resolution_clock::now();
    }
};

void Timer::merge(Timer timer) {
    if (running) {
        NES_DEBUG("Timer: Trying to merge while timer is running so will skip this operation");
    } else {
        this->runtime += timer.runtime;
        for (auto& s : timer.getSnapshots())
            snapshots.push_back(s);
    }
};

unsigned int Timer::getRuntime() const {
    if (!running) {
        return runtime.count();
    } else {
        NES_DEBUG("Timer: Trying get runtime while timer is running so will return zero");
        return 0;
    }
};

std::ostream & operator<<(std::ostream & Str, const Timer& t) {
    Str << "overall runtime: " << t.getRuntime() << "\n";
    for (auto& s : t.getSnapshots()) {
        Str << s.first + ": " << s.second.count() << " ns" << "\n";
    }
    return Str;
};

}// namespace NES
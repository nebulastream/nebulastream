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

#include <Util/Timer.hpp>

namespace NES {

template<class timeUnit>
void Timer<timeUnit>::start() {
    if (running) {
        NES_DEBUG("Timer: Trying to start an already running timer so will skip this operation");
    } else {
        running = true;
        start_p = std::chrono::high_resolution_clock::now();
    }
};

template<class timeUnit>
void Timer<timeUnit>::pause() {
    if (!running) {
        NES_DEBUG("Timer: Trying to stop an already stopped timer so will skip this operation");
    } else {
        running = false;

        stop_p = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<timeUnit>(stop_p - start_p);

        pausedDuration += duration;
        runtime += duration;
    }
};

template<class timeUnit>
void Timer<timeUnit>::snapshot(std::string snapshotName) {
    if (!running) {
        NES_DEBUG("Timer: Trying to take a snapshot of an non-running timer so will skip this operation");
    } else {
        running = true;

        stop_p = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<timeUnit>(stop_p - start_p);

        runtime += duration;

        const struct snapshot s = {.name = componentName + '_' + snapshotName,
                                   .runtime = duration,
                                   .children = std::vector<struct snapshot>()};
        snapshots.push_back(s);

        start_p = std::chrono::high_resolution_clock::now();
    }
};

template<class timeUnit>
void Timer<timeUnit>::merge(Timer timer) {
    if (running) {
        NES_DEBUG("Timer: Trying to merge while timer is running so will skip this operation");
    } else {

        this->runtime += timer.runtime;

        const struct snapshot s = {.name = componentName + '_' + timer.getComponentName(),
                                   .runtime = timer.runtime,
                                   .children = timer.getSnapshots()};

        this->snapshots.push_back(s);
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
std::string Timer<timeUnit>::printHelper(std::string str, struct snapshot s) {
    std::ostringstream ostr;
    ostr << str;
    ostr << s.name + ": " << s.getRuntime() << '\n';
    for (auto& c : s.children) {
        ostr << printHelper(str, c);
    }
    return ostr.str();
}

template void Timer<std::chrono::nanoseconds>::start();
template void Timer<std::chrono::nanoseconds>::pause();
template void Timer<std::chrono::nanoseconds>::snapshot(std::string snapshotName);
template void Timer<std::chrono::nanoseconds>::merge(Timer timer);
template unsigned int Timer<std::chrono::nanoseconds>::getRuntime() const;
template std::string Timer<std::chrono::nanoseconds>::printHelper(std::string str, struct snapshot s);

template void Timer<std::chrono::milliseconds>::start();
template void Timer<std::chrono::milliseconds>::pause();
template void Timer<std::chrono::milliseconds>::snapshot(std::string snapshotName);
template void Timer<std::chrono::milliseconds>::merge(Timer timer);
template unsigned int Timer<std::chrono::milliseconds>::getRuntime() const;
template std::string Timer<std::chrono::milliseconds>::printHelper(std::string str, struct snapshot s);

template void Timer<std::chrono::seconds>::start();
template void Timer<std::chrono::seconds>::pause();
template void Timer<std::chrono::seconds>::snapshot(std::string snapshotName);
template void Timer<std::chrono::seconds>::merge(Timer timer);
template unsigned int Timer<std::chrono::seconds>::getRuntime() const;
template std::string Timer<std::chrono::seconds>::printHelper(std::string str, struct snapshot s);
}// namespace NES
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

#include <Types/TimeBasedWindowType.hpp>
#include <Types/WindowType.hpp>
#include <StatisticCollection/StatisticRegistry/StatisticInfo.hpp>
#include <StatisticCollection/StatisticRegistry/StatisticRegistry.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Statistic {

StatisticRegistryPtr StatisticRegistry::create() {
    return std::make_shared<StatisticRegistry>();
}

StatisticInfoWLock StatisticRegistry::getStatisticInfo(const StatisticHash statisticHash) {
    auto lockedMap = keyToStatisticInfo.wlock();
    auto lockedStatisticInfo = (*lockedMap)[statisticHash].wlock();
    return std::make_shared<folly::Synchronized<StatisticInfo>::WLockedPtr>(std::move(lockedStatisticInfo));
}

QueryId StatisticRegistry::getQueryId(const StatisticHash statisticHash) const {
    auto lockedMap = keyToStatisticInfo.rlock();
    return (*lockedMap).at(statisticHash)->getQueryId();
}

std::optional<StatisticInfoWLock> StatisticRegistry::getStatisticInfoWithGranularity(const StatisticHash statisticHash,
                                                                                     const Windowing::TimeMeasure& granularity) {
    // If there exists no StatisticInfo to this StatisticHash, then return no value
    if (!contains(statisticHash)) {
        return {};
    }

    // Checking if the window is a TimeBasedWindowType, as we currently on support those
    const auto statisticInfo = getStatisticInfo(statisticHash);
    const auto window = (*statisticInfo)->getWindow();
    if (!window->instanceOf<Windowing::TimeBasedWindowType>()) {
        NES_WARNING("Can only infer the granularity of a TimeBasedWindowType.");
        return {};
    }

    // Checking if the window has the same size as the expected granularity
    const auto timeBasedWindow = window->as<Windowing::TimeBasedWindowType>();
    if (timeBasedWindow->getSize() != granularity) {
        return {};
    }
    return statisticInfo;
}

void StatisticRegistry::insert(const StatisticHash statisticHash, const StatisticInfo statisticInfo) {
    auto lockedMap = keyToStatisticInfo.wlock();
    (*lockedMap).insert_or_assign(statisticHash, statisticInfo);
}

bool StatisticRegistry::contains(const StatisticHash statisticHash) {
    auto lockedMap = keyToStatisticInfo.rlock();
    return lockedMap->contains(statisticHash);
}

void StatisticRegistry::queryStopped(const StatisticHash statisticHash) {
    auto lockedMap = keyToStatisticInfo.wlock();
    (*lockedMap)[statisticHash]->stoppedQuery();
}

void StatisticRegistry::clear() { (*keyToStatisticInfo.wlock()).clear(); }

bool StatisticRegistry::isRunning(const StatisticHash statisticHash) const {
    auto lockedMap = keyToStatisticInfo.rlock();
    return (*lockedMap).at(statisticHash)->isRunning();
}

}// namespace NES::Statistic
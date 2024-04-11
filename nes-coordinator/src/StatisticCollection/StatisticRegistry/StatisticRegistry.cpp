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

#include <Operators/LogicalOperators/Windows/Types/TimeBasedWindowType.hpp>
#include <Operators/LogicalOperators/Windows/Types/WindowType.hpp>
#include <StatisticCollection/StatisticRegistry/StatisticInfo.hpp>
#include <StatisticCollection/StatisticRegistry/StatisticRegistry.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Statistic {
StatisticInfoWLock StatisticRegistry::getStatisticInfo(const StatisticKey statisticKey) {
    auto lockedMap = keyToStatisticInfo.wlock();
    auto lockedStatisticInfo = (*lockedMap)[statisticKey].wlock();
    return std::make_shared<folly::Synchronized<StatisticInfo>::WLockedPtr>(std::move(lockedStatisticInfo));
}

QueryId StatisticRegistry::getQueryId(const StatisticKey statisticKey) const {
    auto lockedMap = keyToStatisticInfo.rlock();
    return (*lockedMap).at(statisticKey)->getQueryId();
}

std::optional<StatisticInfoWLock> StatisticRegistry::getStatisticInfoWithGranularity(const StatisticKey statisticKey,
                                                                                     const Windowing::TimeMeasure& granularity) {
    // If there exists no StatisticInfo to this StatisticKey, then return no value
    if (!contains(statisticKey)) {
        return {};
    }

    // Checking if the window is a TimeBasedWindowType, as we currently on support those
    const auto statisticInfo = getStatisticInfo(statisticKey);
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

std::vector<StatisticInfoWLock> StatisticRegistry::getStatisticInfo(const QueryId queryId) {
    auto lockedMap = keyToStatisticInfo.wlock();
    std::vector<StatisticInfoWLock> statisticInfos;
    for (auto& statisticInfo : (*lockedMap)) {
        auto lockedStatisticInfo = statisticInfo.second.wlock();
        if (lockedStatisticInfo->getQueryId() == queryId) {
            statisticInfos.emplace_back(
                std::make_shared<folly::Synchronized<StatisticInfo>::WLockedPtr>(std::move(lockedStatisticInfo)));
        }
    }
    return statisticInfos;
}

void StatisticRegistry::insert(const StatisticKey statisticKey, const StatisticInfo statisticInfo) {
    auto lockedMap = keyToStatisticInfo.wlock();
    (*lockedMap).insert_or_assign(statisticKey, statisticInfo);
}

bool StatisticRegistry::contains(const StatisticKey statisticKey) {
    auto lockedMap = keyToStatisticInfo.rlock();
    return lockedMap->contains(statisticKey);
}

void StatisticRegistry::queryStopped(const StatisticKey statisticKey) {
    auto lockedMap = keyToStatisticInfo.wlock();
    (*lockedMap)[statisticKey]->stoppedQuery();
}

void StatisticRegistry::clear() { (*keyToStatisticInfo.wlock()).clear(); }

bool StatisticRegistry::isRunning(const StatisticKey statisticKey) const {
    auto lockedMap = keyToStatisticInfo.rlock();
    return (*lockedMap).at(statisticKey)->isRunning();
}

}// namespace NES::Statistic
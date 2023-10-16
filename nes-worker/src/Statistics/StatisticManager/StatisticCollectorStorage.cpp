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

#include <Statistics/StatisticManager/StatisticCollectorStorage.hpp>

namespace NES::Experimental::Statistics {

StatisticCollectorStorage::StatisticCollectorStorage() {};

void StatisticCollectorStorage::createStatistic(const StatisticCollectorIdentifier& statisticCollectorIdentifier, const double statistic) {
    trackedStatistics[statisticCollectorIdentifier] = statistic;
    return;
}

double StatisticCollectorStorage::probeStatistic(const StatisticCollectorIdentifier& statisticCollectorIdentifier) {
    auto statIt = trackedStatistics.find(statisticCollectorIdentifier);
    if (statIt != trackedStatistics.end()) {
        return statIt->second;
    } else {
        // ToDo create Statistic object to return instead. Issue:4354
        return -1.0;
    }
}
void StatisticCollectorStorage::deleteStatistic(const StatisticCollectorIdentifier& statisticCollectorIdentifier) {
    trackedStatistics.erase(statisticCollectorIdentifier);
}
}

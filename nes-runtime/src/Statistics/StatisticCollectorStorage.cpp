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

#include <Statistics/CountMin.hpp>
#include <Statistics/ReservoirSample.hpp>
#include <Statistics/Statistic.hpp>
#include <Statistics/StatisticCollectorStorage.hpp>
#include <Util/StatisticCollectorIdentifier.hpp>

namespace NES::Experimental::Statistics {

StatisticCollectorStorage::StatisticCollectorStorage() = default;

void StatisticCollectorStorage::addStatistic(StatisticCollectorIdentifier& statisticCollectorIdentifier,
                                             const StatisticPtr statistic) {
    trackedStatistics[statisticCollectorIdentifier] = statistic;
    return;
}

void StatisticCollectorStorage::addStatistics(std::vector<StatisticPtr> allStatistics) {
    for (auto& statistic : allStatistics) {
        addStatistic(*(statistic->getStatisticCollectorIdentifier().get()),
                     std::dynamic_pointer_cast<Statistic>(std::shared_ptr<Statistic>(statistic)));
    }
}

double StatisticCollectorStorage::probeStatistic(const StatisticCollectorIdentifier& statisticCollectorIdentifier) {
    auto statIt = trackedStatistics.find(statisticCollectorIdentifier);
    if (statIt != trackedStatistics.end()) {
        auto statistic = statIt->second.get();
        if (nullptr != dynamic_cast<CountMin*>(statistic)) {
            auto cm = dynamic_cast<CountMin*>(statistic);
            auto cmData = cm->getData();
            return cmData[0][0];
        }
        // ToDo: add probe logic
        return 1.0;
    } else {
        // ToDo create Statistic object to return instead. Issue:4354
        return -1.0;
    }
}
void StatisticCollectorStorage::deleteStatistic(const StatisticCollectorIdentifier& statisticCollectorIdentifier) {
    trackedStatistics.erase(statisticCollectorIdentifier);
}
}// namespace NES::Experimental::Statistics

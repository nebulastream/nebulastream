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

#include <limits>

#include <Statistics/StatisticCollectorStorage.hpp>
#include <Statistics/StatisticManager/StatisticManager.hpp>
#include <Util/StatisticCollectorIdentifier.hpp>

namespace NES::Experimental::Statistics {

StatisticManager::StatisticManager() : statisticCollectorStorage(std::make_shared<StatisticCollectorStorage>()) {}

StatisticCollectorStoragePtr StatisticManager::getStatisticCollectorStorage() { return statisticCollectorStorage; }

void StatisticManager::probeStatistic(const std::string& logicalSourceName,
                                      const std::vector<std::string>& allPhyicalSourceNames,
                                      WorkerId workerId,
                                      const std::string& fieldName,
                                      StatisticCollectorType statisticCollectorType,
                                      ProbeStatisticReply* allStatistics) {

    double statistic;

    for (auto physicalSourceName : allPhyicalSourceNames) {
        auto statCollectorIdentifier =
            StatisticCollectorIdentifier(logicalSourceName, physicalSourceName, workerId, fieldName, statisticCollectorType);

        statistic = statisticCollectorStorage->probeStatistic(statCollectorIdentifier);
        // ToDo: add try catch block
        if (statistic != std::numeric_limits<double>::quiet_NaN()) {
            allStatistics->add_statistics(statistic);
        } else {
            allStatistics->Clear();
            return;
        }
    }
    return;
}

bool StatisticManager::deleteStatistic(const std::string& logicalSourceName,
                                       const std::vector<std::string>& allPhysicalSourceNames,
                                       WorkerId workerId,
                                       const std::string& fieldName,
                                       StatisticCollectorType statisticCollectorType) {

    for (auto physicalSourceName : allPhysicalSourceNames) {
        auto statisticCollectorIdentifier = StatisticCollectorIdentifier(logicalSourceName,
                                                                         physicalSourceName,
                                                                         workerId,
                                                                         fieldName,
                                                                         statisticCollectorType);

        statisticCollectorStorage->deleteStatistic(statisticCollectorIdentifier);
    }
    return true;
}

}// namespace NES::Experimental::Statistics

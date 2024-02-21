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

#include <Statistics/StatisticCollectorStorage.hpp>
#include <Statistics/StatisticManager/StatisticManager.hpp>
#include <Statistics/SynopsisProbeParameter/CountMinProbeParameter.hpp>
#include <Statistics/SynopsisProbeParameter/StatisticProbeParameter.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/StatisticCollectorIdentifier.hpp>
#include <limits>

namespace NES::Experimental::Statistics {

StatisticManager::StatisticManager() : statisticCollectorStorage(std::make_shared<StatisticCollectorStorage>()) {}

StatisticCollectorStoragePtr StatisticManager::getStatisticCollectorStorage() { return statisticCollectorStorage; }

void StatisticManager::probeStatistic(const std::string& logicalSourceName,
                                      const std::string& fieldName,
                                      StatisticCollectorType statisticCollectorType,
                                      ExpressionNodePtr& expression,
                                      const std::vector<std::string>& allPhyicalSourceNames,
                                      const time_t startTime,
                                      const time_t endTime,
                                      ProbeStatisticReply* allStatistics) {

    double statistic;
    StatisticProbeParameterPtr probeParams = nullptr;

    switch (statisticCollectorType) {
        case StatisticCollectorType::COUNT_MIN: probeParams = std::make_shared<CountMinProbeParameter>(expression);
        case StatisticCollectorType::DDSKETCH: NES_ERROR("Sketch not yet implemented");
        case StatisticCollectorType::HYPER_LOG_LOG: NES_ERROR("Sketch not yet implemented");
        case StatisticCollectorType::RESERVOIR: NES_ERROR("Reservoir not yet implemented");
        default: NES_ERROR("StatisticCollectorType not implemented!")
    }

    for (const auto& physicalSourceName : allPhyicalSourceNames) {
        auto statisticCollectorIdentifier = StatisticCollectorIdentifier(logicalSourceName,
                                                                         physicalSourceName,
                                                                         fieldName,
                                                                         startTime,
                                                                         endTime,
                                                                         statisticCollectorType);

        statistic = statisticCollectorStorage->probeStatistic(statisticCollectorIdentifier, probeParams);
        // ToDo: add try catch block
        if (statistic != std::numeric_limits<double>::quiet_NaN()) {
            allStatistics->add_statistics(statistic);
        } else {
            allStatistics->Clear();
            return;
        }
    }
}

bool StatisticManager::deleteStatistic(const std::string& logicalSourceName,
                                       const std::vector<std::string>& allPhysicalSourceNames,
                                       const std::string& fieldName,
                                       const time_t startTime,
                                       const time_t endTime,
                                       StatisticCollectorType statisticCollectorType) {

    for (const auto& physicalSourceName : allPhysicalSourceNames) {
        auto statisticCollectorIdentifier = StatisticCollectorIdentifier(logicalSourceName,
                                                                         physicalSourceName,
                                                                         fieldName,
                                                                         startTime,
                                                                         endTime,
                                                                         statisticCollectorType);

        statisticCollectorStorage->deleteStatistic(statisticCollectorIdentifier);
    }
    return true;
}
}// namespace NES::Experimental::Statistics

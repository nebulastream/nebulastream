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

#include <Statistics/Requests/StatisticDeleteRequest.hpp>
#include <Statistics/Requests/StatisticProbeRequest.hpp>
#include <Statistics/StatisticManager/StatisticCollectorIdentifier.hpp>
#include <Statistics/StatisticManager/StatisticCollectorStorage.hpp>
#include <Statistics/StatisticManager/StatisticManager.hpp>

namespace NES::Experimental::Statistics {

StatisticManager::StatisticManager() : statisticCollectorStorage(std::make_unique<StatisticCollectorStorage>()) {}

StatisticCollectorStoragePtr StatisticManager::getStatisticCollectorStorage() { return statisticCollectorStorage; }

void StatisticManager::probeStatistic(StatisticProbeRequest& probeRequest,
                                      ProbeStatisticReply* allStatistics,
                                      WorkerId nodeID) {

    auto allPhysicalSourceNames = probeRequest.getPhysicalSourceNames();
    double statistic;

    for (auto physicalSourceName : probeRequest.getPhysicalSourceNames()) {
        auto statCollectorIdentifier = StatisticCollectorIdentifier(probeRequest.getLogicalSourceName(),
                                                                    physicalSourceName,
                                                                    nodeID,
                                                                    probeRequest.getFieldName(),
                                                                    probeRequest.getStatisticCollectorType());

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

bool StatisticManager::deleteStatistic(StatisticDeleteRequest& deleteRequest, WorkerId nodeID) {

    for (auto physicalSourceName : deleteRequest.getPhysicalSourceNames()) {
        auto statisticCollectorIdentifier = StatisticCollectorIdentifier(deleteRequest.getLogicalSourceName(),
                                                                         physicalSourceName,
                                                                         nodeID,
                                                                         deleteRequest.getFieldName(),
                                                                         deleteRequest.getStatisticCollectorType());

        statisticCollectorStorage->deleteStatistic(statisticCollectorIdentifier);
    }
    return true;
}

}// namespace NES::Experimental::Statistics

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
#include <Execution/StatisticsCollector/BranchMisses.hpp>
#include <Execution/StatisticsCollector/CollectorTrigger.hpp>
#include <Execution/StatisticsCollector/Statistic.hpp>
#include <Execution/StatisticsCollector/StatisticsCollector.hpp>
#include <string>

namespace NES::Runtime::Execution {

void StatisticsCollector::addStatistic(uint64_t id, std::shared_ptr<Statistic> statistic){
    if (idToStatisticMap.find(id) == idToStatisticMap.end()){
        idToStatisticMap.insert(std::make_pair(id,std::vector<std::shared_ptr<Statistic>>{statistic}));
    } else {
        idToStatisticMap[id].push_back(statistic);
    }
}

void StatisticsCollector::updateStatisticsHandler(CollectorTrigger trigger) {
    uint64_t id = trigger.getId();

    if (id != 0) {
        auto statisticsMapEntry = idToStatisticMap.find(id);

        if (statisticsMapEntry != idToStatisticMap.end()) {
            for (const std::shared_ptr<Statistic>& statistic : statisticsMapEntry->second) {
                statistic->collect();
                // if collect returns true, change detected -> report back to compiler and optimizer
            }
        }
    }
}

} // namespace NES::Runtime::Execution
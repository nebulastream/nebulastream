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
#include <Execution/StatisticsCollector/CollectorTrigger.hpp>
#include <Execution/StatisticsCollector/PipelineRuntime.hpp>
#include <Execution/StatisticsCollector/PipelineSelectivity.hpp>
#include <Execution/StatisticsCollector/PerformanceStatistics.hpp>
#include <Execution/StatisticsCollector/Statistic.hpp>
#include <Execution/StatisticsCollector/StatisticsCollector.hpp>
#include <string>

namespace NES::Runtime::Execution {

void StatisticsCollector::addStatistic(std::shared_ptr<Statistic> statistic) {
    listOfStatistics.push_back(statistic);
}

void StatisticsCollector::updateStatisticsHandler(CollectorTrigger trigger) {

    TriggerType type = trigger.getTriggerType();
    std::string statisticType;
    for (const auto& stat : listOfStatistics) {
        statisticType = stat->getType();
        if(type == Execution::PipelineStatisticsTrigger){
            if (statisticType == "PipelineSelectivity" ){
                stat->collect();
                //auto pipelineSelectivity = std::dynamic_pointer_cast<PipelineSelectivity>(stat);
                //pipelineSelectivity->collect();
            } else if (statisticType == "PipelineRuntime" ){
                auto pipelineRuntime = std::dynamic_pointer_cast<PipelineRuntime>(stat);
                pipelineRuntime->collect();
            }
            break ;
        } else if (type == Execution::PerformanceStartTrigger && statisticType == "PerformanceStatistics") {
            auto perfStatistics = std::dynamic_pointer_cast<PerformanceStatistics>(stat);
            perfStatistics->startProfiling();
            break ;
        } else if (type == Execution::PerformanceEndTrigger && statisticType == "PerformanceStatistics") {
            stat->collect();
        }
    }
}

} // namespace NES::Runtime::Execution
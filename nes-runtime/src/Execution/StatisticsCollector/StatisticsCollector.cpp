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
#include <Execution/StatisticsCollector/StatisticsCollector.hpp>
#include <Execution/StatisticsCollector/Statistic.hpp>
#include <Execution/StatisticsCollector/CollectorTrigger.hpp>
#include <Execution/StatisticsCollector/PipelineSelectivity.hpp>
#include <Execution/StatisticsCollector/PipelineRuntime.hpp>

namespace NES::Runtime::Execution {

void StatisticsCollector::addStatistic(std::shared_ptr<Statistic> statistic) {
    listOfStatistics.push_back(statistic);
}

void StatisticsCollector::updateStatisticsHandler(CollectorTrigger trigger) {

    if(trigger.getTriggerType() == Execution::PipelineStatisticsTrigger){
        for (auto stat : listOfStatistics) {
            if (stat->getType() == "PipelineSelectivity" ){
                auto pipelineSelectivity = std::dynamic_pointer_cast<PipelineSelectivity>(stat);
                pipelineSelectivity->collect();
            }
            if (stat->getType() == "PipelineRuntime" ){
                auto pipelineRuntime = std::dynamic_pointer_cast<PipelineRuntime>(stat);
                pipelineRuntime->collect();
            }
        }
    }


}

} // namespace NES::Runtime::Execution
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

#include "ThroughputTuplePerTaskComputer.hpp"
#include <unordered_map>
#include <ranges>

#include "../TaskStatisticsProcessor.hpp"

namespace NES::Runtime {
void ThroughputTuplePerTaskComputer::calculateOptimalTaskSize(std::unordered_map<PipelineId, PipelineStatistics>& pipelineStatisticsLocked, std::unordered_map<QueryId, QueryInfo>& queryInfoLocked) const
{
    /// We iterate through all queries and check if the throughput is higher/lower than the min throughput.
    /// If it is lower, we increase the task size, if it is higher, we decrease the task size.
    for (auto queryInfo : queryInfoLocked | std::views::values)
    {
        if (queryInfo.currentThroughput < queryInfo.sla.minThroughput)
        {
            for (const auto& pipelineId : queryInfo)
            {
                auto& pipelineStatistics = pipelineStatisticsLocked[pipelineId];
                pipelineStatistics.changeNextNumberOfTuplesPerTask(increaseTaskSizeFactor);
            }
        }
        else
        {
            for (const auto& pipelineId : queryInfo)
            {
                auto& pipelineStatistics = pipelineStatisticsLocked[pipelineId];
                pipelineStatistics.changeNextNumberOfTuplesPerTask(decreaseTaskSizeFactor);
            }
        }
    }
}
}

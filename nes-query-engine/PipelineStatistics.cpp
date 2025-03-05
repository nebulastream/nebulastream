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

#include "PipelineStatistics.hpp"

namespace NES::Runtime {
void PipelineStatistics::updateTaskStatistics(const TaskStatistics& taskStatistic)
{
    if (storedTaskStatistics.size() == windowSizeRollingAverage)
    {
        const auto& [throughput, latency, numberOfTuples] = storedTaskStatistics.front();
        sumThroughput -= throughput;
        sumLatency -= latency;
        sumNumberOfTuples -= numberOfTuples;
        storedTaskStatistics.pop();
    }
    storedTaskStatistics.push(taskStatistic);
    sumLatency += taskStatistic.latency;
    sumThroughput += taskStatistic.throughput;
    sumNumberOfTuples += taskStatistic.numberOfTuples;
}

double PipelineStatistics::getAverageThroughput() const
{
    return sumThroughput / storedTaskStatistics.size();
}

std::chrono::microseconds PipelineStatistics::getAverageLatency() const
{
    return sumLatency / storedTaskStatistics.size();
}

uint64_t PipelineStatistics::getAverageNumberOfTuples() const
{
    return sumNumberOfTuples / storedTaskStatistics.size();
}

uint64_t PipelineStatistics::getNextNumberOfTuplesPerTask() const
{
    return nextNumberOfTuplesPerTask;
}

void PipelineStatistics::changeNextNumberOfTuplesPerTask(const double factor)
{
    nextNumberOfTuplesPerTask *= factor;
    if (nextNumberOfTuplesPerTask < 1)
    {
        nextNumberOfTuplesPerTask = 1;
    }
}
}

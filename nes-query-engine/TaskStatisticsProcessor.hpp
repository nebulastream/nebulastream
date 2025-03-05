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

#pragma once
#include <queue>
#include <set>
#include <thread>
#include <type_traits>
#include <Listeners/SystemEventListener.hpp>
#include <folly/MPMCQueue.h>
#include <folly/Synchronized.h>
#include <QueryEngineStatisticListener.hpp>

namespace NES::Runtime
{
struct PipelineStatistics
{
    explicit PipelineStatistics(const size_t windowSizeRollingAverage)
        : sumThroughput(0), sumLatency(0), sumNumberOfTuples(0), windowSizeRollingAverage(windowSizeRollingAverage)
    {
    }
    explicit PipelineStatistics() : PipelineStatistics(10) { }

    struct TaskStatistics
    {
        double throughput;
        double latency;
        uint64_t numberOfTuples;
    };

    /// Adding the statistics of a task to the pipeline statistics and calculating the rolling average
    void updateTaskStatistics(const TaskStatistics taskStatistic);
    double getAverageThroughput() const;
    double getAverageLatency() const;

private:
    double sumThroughput;
    double sumLatency;
    uint64_t sumNumberOfTuples;
    std::queue<TaskStatistics> storedTaskStatistics;
    size_t windowSizeRollingAverage;
};

struct QuerySLA
{
    double minThroughput;
    double maxLatency;
};

template <typename Var1, typename Var2>
struct FlattenVariant;
template <typename... Ts1, typename... Ts2>
struct FlattenVariant<std::variant<Ts1...>, std::variant<Ts2...>>
{
    using type = std::variant<Ts1..., Ts2...>;
};

struct TaskStatisticsProcessor final : QueryEngineStatisticListener
{
    using CombinedEventType = FlattenVariant<SystemEvent, Event>::type;

    /// This method is used to get an estimated number of tuples for a pipeline, such that the SLA for the query can be sustained
    uint64_t getNumberOfTuplesPerBuffer(const PipelineId pipelineId) const;


    void onEvent(Event event) override;
    explicit TaskStatisticsProcessor();
    static_assert(std::is_default_constructible_v<CombinedEventType>);

private:
    folly::MPMCQueue<CombinedEventType> events{1000};
    std::jthread printThread;

    /// Storage containers for the statistics per pipeline
    folly::Synchronized<std::unordered_map<PipelineId, PipelineStatistics>> pipelineStatistics;
    folly::Synchronized<std::unordered_map<QueryId, std::set<PipelineId>>> queryPipelines;
    folly::Synchronized<std::unordered_map<QueryId, QuerySLA>> querySLAs;
};
}

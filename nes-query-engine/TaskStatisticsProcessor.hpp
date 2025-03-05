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
#include <memory>
#include <set>
#include <thread>
#include <type_traits>
#include <utility>
#include <Listeners/SystemEventListener.hpp>
#include <folly/MPMCQueue.h>
#include <folly/Synchronized.h>
#include <QueryEngineStatisticListener.hpp>
#include <Identifiers/Identifiers.hpp>
#include "TuplePerTaskComputer/TuplePerTaskComputer.hpp"
#include "PipelineStatistics.hpp"

namespace NES::Runtime
{

struct QuerySLA
{
    QuerySLA() : minThroughput(0), maxLatency(std::numeric_limits<uint64_t>::max()) { }
    double minThroughput;
    std::chrono::microseconds maxLatency;
};

class QueryInfo
{
public:
    explicit QueryInfo() : currentThroughput(0), currentLatency(0) { }

    void addQuerySLA(double minThroughput, std::chrono::microseconds maxLatency);
    void insert(PipelineId pipelineId);
    std::set<PipelineId>::iterator begin() const { return pipelineIds.begin(); }
    std::set<PipelineId>::iterator end() const { return pipelineIds.end(); }

    QuerySLA sla;
    std::set<PipelineId> pipelineIds;
    double currentThroughput;
    std::chrono::microseconds currentLatency;
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

    void onEvent(Event event) override;
    explicit TaskStatisticsProcessor(std::unique_ptr<TuplePerTaskComputer> tuplePerTaskComputer);
    static_assert(std::is_default_constructible_v<CombinedEventType>);

    /// This method is used to get the estimated number of tuples for a pipeline, such that the SLA for the query can be sustained
    uint64_t getNumberOfTuplesPerBuffer(const PipelineId pipelineId) const;

protected:
    /// Storage containers for the statistics per pipeline
    folly::Synchronized<std::unordered_map<PipelineId, PipelineStatistics>> pipelineStatistics;
    folly::Synchronized<std::unordered_map<QueryId, QueryInfo>> queryInfo;

private:
    std::unique_ptr<TuplePerTaskComputer> tuplePerTaskComputer;
    folly::MPMCQueue<CombinedEventType> events{1000};
    std::jthread printThread;
};

}

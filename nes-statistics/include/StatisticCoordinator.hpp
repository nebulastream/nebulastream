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

#include <atomic>
#include <cstdint>
#include <expected>
#include <functional>
#include <future>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <Identifiers/Identifiers.hpp>
#include <Plans/LogicalPlan.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <folly/Synchronized.h>
#include <ErrorHandling.hpp>
#include <RequestStatisticStatement.hpp>
#include <Statistic.hpp>
#include <StatisticQueryGenerator.hpp>
#include <StatisticRegistry.hpp>

namespace NES
{

/// Result of a collectNewStatistic() call.
struct CollectStatisticResult
{
    QueryId queryId;
    Statistic::StatisticId statisticId;
    bool alreadyExisted;
};

/// Central coordinator for statistic requests (control plane). Owns the StatisticRegistry and assigns unique
/// StatisticIds. Data-plane results arrive over a named pipe (FIFO): the data-plane queries it submits write
/// CSV records `statisticId,startTs,endTs,value` into the FIFO via a FileSink, and a reader thread here parses
/// them and routes to pending probes / condition triggers. (This is the gRPC-free PoC transport; the gRPC
/// server + per-probe request trigger of the upstream version are replaced by the FIFO reader + per-call probe
/// query generation.)
class StatisticCoordinator
{
public:
    /// Callback that submits an already-generated LogicalPlan, returning the QueryId on success.
    using SubmitQueryFn = std::function<std::expected<QueryId, Exception>(LogicalPlan)>;

    StatisticCoordinator(std::unique_ptr<StatisticQueryGenerator> queryGenerator, SubmitQueryFn submitQuery);
    ~StatisticCoordinator();

    StatisticCoordinator(const StatisticCoordinator&) = delete;
    StatisticCoordinator& operator=(const StatisticCoordinator&) = delete;
    StatisticCoordinator(StatisticCoordinator&&) = delete;
    StatisticCoordinator& operator=(StatisticCoordinator&&) = delete;

    /// Requests collection of a new statistic. Deduplicates on (metric, collectionDomain, windowSize): if an
    /// identical request is active it returns the existing entry (and appends the trigger if provided). Otherwise
    /// assigns a StatisticId, generates the build query (writing into the FIFO), submits it, and registers it.
    [[nodiscard]] std::expected<CollectStatisticResult, Exception> collectNewStatistic(const RequestStatisticBuildStatement& statement);

    /// Adds a condition trigger to an existing statistic entry. Returns false if the key is not found.
    bool addConditionTrigger(const StatisticRegistry::Key& key, ConditionTrigger trigger);

    /// Removes the entry for this key. Returns true if an entry was removed.
    bool deregisterStatistic(const StatisticRegistry::Key& key);

    /// Creates the result FIFO (mkfifo) and starts the reader thread. Must be called before collectNewStatistic /
    /// getStatistics (the generated queries bake the returned FIFO path into their FileSink). Returns the path.
    std::string startResultReader();

    /// Stops the reader thread and removes the FIFO.
    void stopResultReader();

    [[nodiscard]] const std::string& getResultFifoPath() const { return fifoPath; }

    /// Submits a probe query for `key` over [startTs, endTs] and waits (with timeout) for the value to arrive
    /// over the FIFO. Returns std::nullopt if no result was received in time.
    std::optional<double> getStatistics(const StatisticRegistry::Key& key, Windowing::TimeMeasure startTs, Windowing::TimeMeasure endTs);

    /// Called by the reader thread when a CSV result record arrives. Routes to a pending probe or condition triggers.
    void onStatisticReport(Statistic::StatisticId statisticId, Windowing::TimeMeasure startTs, Windowing::TimeMeasure endTs, double value);

private:
    void readerLoop();

    std::atomic<uint64_t> nextStatisticId{1};
    StatisticRegistry registry;
    std::unique_ptr<StatisticQueryGenerator> queryGenerator;
    SubmitQueryFn submitQuery;

    std::string fifoPath;
    std::thread readerThread;
    std::atomic<bool> readerRunning{false};

    struct PendingProbe
    {
        std::promise<double> promise;
    };
    folly::Synchronized<std::unordered_map<Statistic::StatisticId, PendingProbe>> pendingProbes;
};

}

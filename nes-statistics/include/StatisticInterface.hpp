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
#include <condition_variable>
#include <mutex>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>
#include <ConditionTrigger.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Plans/LogicalPlan.hpp>
#include <RequestStatisticStatement.hpp>
#include <StatisticTuple.hpp>
#include <StatisticQueryGenerator.hpp>
#include <StatisticRegistry.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <ErrorHandling.hpp>

namespace grpc
{
class Server;
}

namespace NES
{

/// Result of a collectNewStatistic() call.
struct CollectStatisticResult
{
    QueryId queryId;
    StatisticTuple::StatisticId statisticId;
    bool alreadyExisted;
};

/// Central statistic interface for statistic requests. Owns the StatisticRegistry and hands out unique StatisticIds.
///
/// Also runs a gRPC server (StatisticInterfaceService) that receives results from gRPC sinks. Because the
/// service implementation holds a reference to the statistic interface, an instance must not move after
/// startGrpcServer() has been called.
class StatisticInterface
{
public:
    /// Takes a generated LogicalPlan and submits it, returning the QueryId or an Exception.
    using SubmitQueryFn = std::function<std::expected<QueryId, Exception>(LogicalPlan)>;

    StatisticInterface(std::unique_ptr<StatisticQueryGenerator> queryGenerator, SubmitQueryFn submitQuery);
    ~StatisticInterface();

    /// Non-movable and non-copyable: StatisticInterfaceServiceImpl captures `this`.
    StatisticInterface(const StatisticInterface&) = delete;
    StatisticInterface& operator=(const StatisticInterface&) = delete;
    StatisticInterface(StatisticInterface&&) = delete;
    StatisticInterface& operator=(StatisticInterface&&) = delete;

    /// Requests collection of a new statistic. If an identical request is already active (same metric, collection
    /// domain, window size) the existing entry is returned and the trigger, if any, is appended to it. Otherwise a
    /// fresh StatisticId is allocated, the collection query is generated and submitted, and the entry registered.
    [[nodiscard]] std::expected<CollectStatisticResult, Exception> collectNewStatistic(const RequestStatisticBuildStatement& statement);

    /// Adds a condition trigger to an existing entry. Returns false if the key is not registered.
    bool addConditionTrigger(const StatisticRegistry::Key& key, ConditionTrigger trigger);

    /// Removes the entry for this key. Returns true if one was removed.
    bool deregisterStatistic(const StatisticRegistry::Key& key);

    /// Starts the gRPC server on a kernel-chosen port and returns its "host:port".
    std::string startGrpcServer();

    void stopGrpcServer();

    [[nodiscard]] const std::string& getInterfaceAddress() const { return interfaceAddress; }

    /// Probes statistics that have already been collected.
    ///
    /// Deploys a probe query (GrpcSource -> ScalarStatisticProbe -> GrpcSink), pushes one RequestStatistic per
    /// key into its source, and waits for the reports to come back through onStatisticReport. Returns the sum of
    /// the reported values, or nullopt if they did not all arrive before the timeout.
    std::optional<double>
    getStatistics(const std::vector<StatisticRegistry::Key>& keys, Windowing::TimeMeasure startTs, Windowing::TimeMeasure endTs);

    /// Called by the gRPC service handler when a StatisticReport arrives. Routes to a pending probe if one is
    /// waiting on that id, otherwise to the registry's condition triggers.
    void onStatisticReport(StatisticTuple::StatisticId statisticId, Windowing::TimeMeasure startTs, Windowing::TimeMeasure endTs, double value);

    /// How long getStatistics waits for a probed statistic to report anything at all.
    static constexpr auto PROBE_TIMEOUT = std::chrono::seconds{30};

    /// How long it keeps collecting after the first report of a probe arrives.
    ///
    /// A probe over a time range emits one row per stored window, and nothing tells the statistic interface up front how
    /// many that will be, so there is no count to wait for. The rows arrive in one burst from a single impulse,
    /// so waiting a short settling interval after the first one collects the rest.
    static constexpr auto PROBE_SETTLE_INTERVAL = std::chrono::milliseconds{500};

private:
    std::atomic<uint64_t> nextStatisticId{1};
    StatisticRegistry registry;
    std::unique_ptr<StatisticQueryGenerator> queryGenerator;
    SubmitQueryFn submitQuery;

    std::unique_ptr<grpc::Server> grpcServer;
    /// Type-erased so this header does not need the generated service definition.
    std::shared_ptr<void> grpcService;
    std::string interfaceAddress;

    /// Accumulates every report for one probed statistic rather than just the first, because a range probe
    /// legitimately produces several.
    struct PendingProbe
    {
        double sum{0};
        uint64_t reports{0};
    };

    std::mutex probeMutex;
    std::condition_variable probeCv;
    std::unordered_map<StatisticTuple::StatisticId, PendingProbe> pendingProbes;
};

}

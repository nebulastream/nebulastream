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

#include <chrono>
#include <cstddef>
#include <functional>
#include <span>
#include <string_view>
#include <thread>
#include <vector>

#include <nes-coordinator-bridge/coordinator.h>

#include <Config/Config.hpp>
#include <Model/RunnablePartition.hpp>
#include <Model/RunnableTestFile.hpp>
#include <Model/Verdict.hpp>
#include <Runner/Cluster.hpp>

namespace NES
{

/// Sets up the prepared partitions when constructed, then submits and checks their test cases.
/// Each submission runs every test case once more.
/// A caller repeats a run by submitting again, because a second setup would fail on the repeated CREATEs.
class TestRunner
{
public:
    /// A file whose setup fails is reported and not thrown, so the other files still run.
    TestRunner(const SystestConfiguration& config, const Cluster& cluster, std::vector<RunnablePartition> partitions);

    /// One runner per invocation.
    /// Even when we want to submit multiple times, all setup statements need only one submission, which is done in the constructor.
    TestRunner(const TestRunner&) = delete;
    TestRunner(TestRunner&&) = delete;
    TestRunner& operator=(const TestRunner&) = delete;
    TestRunner& operator=(TestRunner&&) = delete;

    /// Called for each test case after it has been checked.
    /// Enables the caller to report progress and record timings for reporting.
    /// The runner calls the observer serially, once per checked test case, so the observer needs no locking.
    using Observer = std::function<void(const ReportEntry&, const RewrittenTestCase&, std::span<const StatementTiming>)>;

    /// Files with a failed setup: one for the failure and one skip per test case.
    /// A separate getter, because setup happens in the constructor, which cannot return values.
    [[nodiscard]] const std::vector<ReportEntry>& getRejected() const;

    /// Counts only the test cases of the partitions that setup accepted.
    [[nodiscard]] size_t countTestCases() const;

    /// Submits the test cases of the set-up partitions, up to `concurrency` at a time, and checks each one.
    /// Returns the entries in file order, so the report does not depend on timing.
    [[nodiscard]] std::vector<ReportEntry> submitAll(size_t concurrency, const Observer& observe = {});

private:
    struct Job
    {
        const RunnablePartition& partition;
        const RewrittenTestCase& testCase;
        size_t reportSlot;
    };

    struct Submitted
    {
        const Job* job = nullptr;
        std::vector<Bridge::StatementOutcome> outcomes;
    };

    void setUp(std::vector<RunnablePartition> partitions);

    [[nodiscard]] Bridge::StatementOutcome submit(std::string_view sql, Bridge::WaitMode wait) const;

    /// Waits until the query has terminated.
    /// Stops a query that runs past the timeout, so it does not keep running on workers that later test cases use.
    [[nodiscard]] Bridge::StatementOutcome runQuery(std::string_view sql) const;

    std::reference_wrapper<const Cluster> cluster;
    std::chrono::milliseconds queryTimeout;
    std::vector<RunnablePartition> setUpPartitions;
    std::vector<ReportEntry> rejected;
    std::vector<std::jthread> servers;
};

}

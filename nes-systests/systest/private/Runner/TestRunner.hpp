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
#include <deque>
#include <functional>
#include <optional>
#include <span>
#include <thread>
#include <unordered_map>
#include <vector>

#include <Config/Config.hpp>
#include <Model/ConfigurationOverride.hpp>
#include <Model/RunnablePartition.hpp>
#include <Model/RunnableTestFile.hpp>
#include <Model/Verdict.hpp>
#include <ResultChecker/OutcomeChecker.hpp>
#include <Runner/QuerySubmitter.hpp>
#include <DistributedQuery.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <SystestBinder.hpp>

namespace NES
{

/// Sets up the prepared partitions when constructed, then submits and checks their test cases.
/// Each submission runs every bound test case once more.
/// A caller repeats a run by submitting again, because a second setup would fail on the repeated CREATEs.
class TestRunner
{
public:
    /// Stages data, writes setup statements into the catalogs, and binds test cases.
    /// A file whose setup fails is reported rather than thrown, so the other files still run.
    TestRunner(const SystestConfiguration& config, std::vector<RunnablePartition> partitions);

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
    struct SetUpPartition
    {
        RunnablePartition partition;
        std::vector<std::vector<BoundStatement>> testCases;
    };

    /// One test case of a set-up partition, and the position of its entry in the report.
    struct Job
    {
        const RunnablePartition& partition;
        const RewrittenTestCase& testCase;
        std::span<const BoundStatement> statements;
        size_t reportSlot;
    };

    /// A test case whose statements the runner submits one after another.
    struct TestCaseRun
    {
        Job job;
        /// The statements that have not finished. While a worker runs a query, the first one is that query.
        std::span<const BoundStatement> remaining;
        std::vector<StatementOutcome> outcomes;
        std::vector<StatementTiming> timings;
    };

    /// The jobs of all partitions that require the same worker settings, so one submitter runs all of them.
    struct WorkerSettingsGroup
    {
        ConfigurationOverride overrides;
        std::deque<Job> jobs;
    };

    /// The test cases of one group that are pending, running, or finished and not yet checked.
    struct GroupSubmission
    {
        QuerySubmitter submitter;
        size_t concurrency;
        std::deque<Job> pending;
        std::unordered_map<DistributedQueryId, TestCaseRun> running;
        std::deque<TestCaseRun> finished;

        [[nodiscard]] bool isDone() const { return pending.empty() and running.empty() and finished.empty(); }
    };

    void setUp(std::vector<RunnablePartition> partitions);

    /// One group at a time: one embedded receiver per process.
    [[nodiscard]] QuerySubmitter createSubmitterFor(const ConfigurationOverride& overrides) const;

    /// Processes an unbound statement or an EXPLAIN in place, without a worker.
    /// @returns nullopt once the test case is ready to check.
    [[nodiscard]] static std::optional<DistributedQueryId> submitNext(QuerySubmitter& submitter, TestCaseRun& run);

    static void admit(GroupSubmission& submission);
    void submitGroup(
        WorkerSettingsGroup group, size_t concurrency, const Observer& observe, std::vector<std::optional<ReportEntry>>& checked) const;

    SystestBinder binder;
    SystestClusterConfiguration clusterConfig;
    bool remote;
    SingleNodeWorkerConfiguration baseWorker;
    std::chrono::milliseconds queryTimeout;
    /// The partitions that setup accepted, which every submit runs.
    std::vector<SetUpPartition> setUpPartitions;
    std::vector<ReportEntry> rejected;
    std::vector<std::jthread> servers;
};

}

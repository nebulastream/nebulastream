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

#include <Runner/TestRunner.hpp>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <deque>
#include <exception>
#include <expected>
#include <iterator>
#include <memory>
#include <optional>
#include <ranges>
#include <span>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <Config/Config.hpp>
#include <Model/ConfigurationOverride.hpp>
#include <Model/RunnablePartition.hpp>
#include <Model/RunnableTestFile.hpp>
#include <Model/TestCaseId.hpp>
#include <Model/Verdict.hpp>
#include <QueryManager/EmbeddedWorkerQuerySubmissionBackend.hpp>
#include <QueryManager/GRPCQuerySubmissionBackend.hpp>
#include <QueryManager/QueryManager.hpp>
#include <ResultChecker/OutcomeChecker.hpp>
#include <Runner/DataStaging.hpp>
#include <Runner/QuerySubmitter.hpp>
#include <DistributedQuery.hpp>
#include <ErrorHandling.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <SystestBinder.hpp>
#include <WorkerCatalog.hpp>

/// In-memory transport: sockets allow one receiver per process.
extern void enable_memcom();

namespace NES
{
namespace
{

SingleNodeWorkerConfiguration applyOverrides(const SingleNodeWorkerConfiguration& base, const ConfigurationOverride& overrides)
{
    auto configured = base;
    for (const auto& [key, value] : overrides)
    {
        configured.overwriteConfigWithCommandLineInput({{key, value}});
    }
    return configured;
}

/// From the running state (not starting) so source and pipeline startup do not count.
std::chrono::nanoseconds extractExecutionTime(const DistributedQueryStatusSnapshot& snapshot)
{
    const auto metrics = snapshot.coalesceQueryMetrics();
    if (not metrics.running.has_value() or not metrics.stop.has_value())
    {
        return std::chrono::nanoseconds::zero();
    }
    return std::max(std::chrono::duration_cast<std::chrono::nanoseconds>(*metrics.stop - *metrics.running), std::chrono::nanoseconds{});
}

TestCaseId createIdOf(const RunnablePartition& partition, const RewrittenTestCase& testCase)
{
    return TestCaseId{.originFile = partition.file.name, .queryIdInFile = getTestCaseNumber(testCase), .overrides = partition.overrides};
}

/// One skip entry per test case of the file, so the report keeps its total.
void appendSkipped(std::vector<ReportEntry>& rejected, const RunnablePartition& partition, const std::string& reason)
{
    for (const auto& testCase : partition.file.testCases)
    {
        rejected.push_back(ReportEntry{.id = createIdOf(partition, testCase), .outcome = Skipped{.reason = reason}});
    }
}

}

TestRunner::TestRunner(const SystestConfiguration& config, std::vector<RunnablePartition> partitions)
    : binder{config}
    , clusterConfig{config.clusterConfig}
    , remote{config.remoteWorker.getValue()}
    , baseWorker{config.singleNodeWorkerConfig.value_or(SingleNodeWorkerConfiguration{})}
{
    if (not config.workerConfig.getValue().empty())
    {
        baseWorker.workerConfiguration.overwriteConfigWithYAMLFileInput(config.workerConfig.getValue());
    }
    if (not remote)
    {
        enable_memcom();
    }
    setUp(std::move(partitions));
}

void TestRunner::setUp(std::vector<RunnablePartition> partitions)
{
    setUpPartitions.reserve(partitions.size());
    for (auto& partition : partitions)
    {
        const auto& [overrides, runnable] = partition;
        if (remote and not overrides.empty())
        {
            appendSkipped(rejected, partition, "a run on workers started elsewhere cannot apply the settings that this file requires");
            continue;
        }
        try
        {
            auto [setupSql, staged] = stage(runnable);
            auto [testCases] = binder.bind(setupSql, runnable.testCases, runnable.key);
            std::ranges::move(staged, std::back_inserter(servers));
            setUpPartitions.push_back(SetUpPartition{.partition = std::move(partition), .testCases = std::move(testCases)});
        }
        catch (const std::exception& exception)
        {
            rejected.push_back(createFailedFileEntry(runnable.name, overrides, "could not run", exception));
            appendSkipped(rejected, partition, "the file's setup failed");
        }
    }
}

QuerySubmitter TestRunner::createSubmitterFor(const ConfigurationOverride& overrides) const
{
    auto backend = remote ? createGRPCBackend() : createEmbeddedBackend(applyOverrides(baseWorker, overrides));
    return QuerySubmitter{std::make_unique<QueryManager>(std::make_shared<WorkerCatalog>(clusterConfig.workers), std::move(backend))};
}

std::optional<DistributedQueryId> TestRunner::submitNext(QuerySubmitter& submitter, TestCaseRun& run)
{
    if (run.remaining.empty())
    {
        return std::nullopt;
    }
    const auto& [plan, explained] = run.remaining.front();
    const auto started = plan.and_then([&](const BoundPlan& bound) { return submitter.startQuery(bound.plan); });
    if (not started.has_value())
    {
        run.outcomes.push_back(StatementOutcome{
            .reached = std::unexpected{started.error()},
            .sinkOutputSchema = plan.has_value() ? std::optional{plan->sinkOutputSchema} : std::nullopt,
            .explained = explained});
        run.timings.emplace_back();
        run.remaining = run.remaining.subspan(1);
        return std::nullopt;
    }
    return *started;
}

void TestRunner::admit(GroupSubmission& submission)
{
    while (submission.running.size() < std::max(submission.concurrency, size_t{1}) and not submission.pending.empty())
    {
        const auto& job = submission.pending.front();
        TestCaseRun run{.job = job, .remaining = job.statements, .outcomes = {}, .timings = {}};
        submission.pending.pop_front();
        if (const auto started = submitNext(submission.submitter, run))
        {
            submission.running.emplace(*started, std::move(run));
        }
        else
        {
            submission.finished.push_back(std::move(run));
        }
    }
}

void TestRunner::submitGroup(
    WorkerSettingsGroup group, const size_t concurrency, const Observer& observe, std::vector<std::optional<ReportEntry>>& checked) const
{
    GroupSubmission submission{
        .submitter = createSubmitterFor(group.overrides),
        .concurrency = concurrency,
        .pending = std::move(group.jobs),
        .running = {},
        .finished = {}};

    const auto check = [&](const TestCaseRun& run)
    {
        const auto& [partition, testCase, _, reportSlot] = run.job;
        ReportEntry entry{
            .id = createIdOf(partition, testCase), .outcome = checkTestCase(run.outcomes, testCase, partition.file.originalNames)};
        if (observe)
        {
            observe(entry, testCase, run.timings);
        }
        checked.at(reportSlot) = std::move(entry);
    };

    while (not submission.isDone())
    {
        /// Check before admitting, so a new query does not run while earlier results are compared, which would distort its timing.
        std::ranges::for_each(submission.finished, check);
        submission.finished.clear();

        admit(submission);
        if (submission.running.empty())
        {
            continue;
        }

        for (auto& snapshot : submission.submitter.finishedQueries())
        {
            auto node = submission.running.extract(snapshot.queryId);
            INVARIANT(not node.empty(), "a finished query was submitted by this run");
            auto& run = node.mapped();

            const auto& [plan, explained] = run.remaining.front();
            const auto executionTime = extractExecutionTime(snapshot);
            const auto stopped = snapshot.getGlobalQueryStatus() == DistributedQueryStatus::Stopped;
            run.outcomes.push_back(StatementOutcome{
                .reached = std::move(snapshot),
                .sinkOutputSchema = plan.has_value() ? std::optional{plan->sinkOutputSchema} : std::nullopt,
                .explained = std::nullopt});
            run.timings.push_back(StatementTiming{.execution = executionTime});
            run.remaining = run.remaining.subspan(1);

            /// Differential halves run in sequence.
            /// A failed first half skips the second, because the second has no result to compare with.
            if (const auto started = stopped ? submitNext(submission.submitter, run) : std::nullopt)
            {
                submission.running.emplace(*started, std::move(run));
            }
            else
            {
                submission.finished.push_back(std::move(run));
            }
        }
    }
}

const std::vector<ReportEntry>& TestRunner::getRejected() const
{
    return rejected;
}

size_t TestRunner::countTestCases() const
{
    return std::ranges::fold_left(
        setUpPartitions, size_t{0}, [](const size_t sum, const SetUpPartition& partition) { return sum + partition.testCases.size(); });
}

std::vector<ReportEntry> TestRunner::submitAll(const size_t concurrency, const Observer& observe)
{
    /// Slots follow file order, although the groups reorder the files.
    size_t reportSlot = 0;
    std::vector<WorkerSettingsGroup> groups;
    for (const auto& [partition, boundTestCases] : setUpPartitions)
    {
        const auto found = std::ranges::find(groups, partition.overrides, &WorkerSettingsGroup::overrides);
        auto& [_, jobs]
            = found == groups.end() ? groups.emplace_back(WorkerSettingsGroup{.overrides = partition.overrides, .jobs = {}}) : *found;
        for (const auto& [testCase, statements] : std::views::zip(partition.file.testCases, boundTestCases))
        {
            jobs.push_back(Job{.partition = partition, .testCase = testCase, .statements = statements, .reportSlot = reportSlot++});
        }
    }

    std::vector<std::optional<ReportEntry>> checked(reportSlot);
    for (auto& group : groups)
    {
        submitGroup(std::move(group), concurrency, observe, checked);
    }
    return checked
        | std::views::transform(
               [](std::optional<ReportEntry>& entry)
               {
                   INVARIANT(entry.has_value(), "every submitted test case was checked");
                   return std::move(*entry);
               })
        | std::ranges::to<std::vector>();
}

}

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

struct TestRunner::Impl
{
    explicit Impl(const SystestConfiguration& config)
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
    }

    struct SetUpPartition
    {
        RunnablePartition partition;
        std::vector<std::vector<BoundStatement>> testCases;
    };

    /// One group at a time: one embedded receiver per process.
    QuerySubmitter createSubmitterFor(const ConfigurationOverride& overrides)
    {
        auto backend = remote ? createGRPCBackend() : createEmbeddedBackend(applyOverrides(baseWorker, overrides));
        return QuerySubmitter{std::make_unique<QueryManager>(std::make_shared<WorkerCatalog>(clusterConfig.workers), std::move(backend))};
    }

    struct Job
    {
        size_t setUpIndex = 0;
        size_t testCaseIndex = 0;
        size_t reportSlot = 0;
    };

    struct InFlight
    {
        Job job;
        size_t statement = 0;
        std::vector<StatementOutcome> outcomes;
        std::vector<StatementTiming> timings;
    };

    [[nodiscard]] const RunnablePartition& getPartitionOf(const Job& job) const { return setUp.at(job.setUpIndex).partition; }

    /// Answers an unbound statement or an EXPLAIN in place, without a worker.
    /// Returns nullopt once the test case is ready to check.
    [[nodiscard]] std::optional<DistributedQueryId> submitNext(QuerySubmitter& submitter, InFlight& flight) const
    {
        const auto& statements = setUp.at(flight.job.setUpIndex).testCases.at(flight.job.testCaseIndex);
        if (flight.statement >= statements.size())
        {
            return std::nullopt;
        }
        const auto& [plan, explained] = statements.at(flight.statement);
        const auto started = plan.and_then([&](const BoundPlan& p) { return submitter.startQuery(p.plan); });
        if (not started.has_value())
        {
            flight.outcomes.push_back(StatementOutcome{
                .reached = std::unexpected{started.error()},
                .sinkOutputSchema = plan.has_value() ? std::optional{plan->sinkOutputSchema} : std::nullopt,
                .explained = explained});
            flight.timings.emplace_back();
            ++flight.statement;
            return std::nullopt;
        }
        return *started;
    }

    /// Fills the window up to `concurrency`, queueing test cases answered without a worker.
    void admit(
        QuerySubmitter& submitter,
        std::deque<Job>& pending,
        const size_t concurrency,
        std::unordered_map<DistributedQueryId, InFlight>& running,
        std::deque<InFlight>& answered) const
    {
        while (running.size() < std::max(concurrency, size_t{1}) and not pending.empty())
        {
            InFlight flight{.job = pending.front(), .statement = 0, .outcomes = {}, .timings = {}};
            pending.pop_front();
            if (const auto started = submitNext(submitter, flight))
            {
                running.emplace(*started, std::move(flight));
            }
            else
            {
                answered.push_back(std::move(flight));
            }
        }
    }

    /// Polls up to `concurrency` in-flight test cases instead of one thread each.
    void submitGroup(
        const ConfigurationOverride& overrides,
        std::deque<Job> pending,
        const size_t concurrency,
        const Observer& observe,
        std::vector<std::optional<ReportEntry>>& checked)
    {
        auto submitter = createSubmitterFor(overrides);
        const auto total = pending.size();
        std::unordered_map<DistributedQueryId, InFlight> running;
        std::deque<InFlight> answered;

        const auto check = [&](const InFlight& flight)
        {
            const auto& partition = getPartitionOf(flight.job);
            const auto& testCase = partition.file.testCases.at(flight.job.testCaseIndex);
            ReportEntry entry{
                .id = createIdOf(partition, testCase), .outcome = checkTestCase(flight.outcomes, testCase, partition.file.originalNames)};
            if (observe)
            {
                observe(entry, testCase, flight.timings);
            }
            checked.at(flight.job.reportSlot) = std::move(entry);
        };

        for (size_t reported = 0; reported < total;)
        {
            admit(submitter, pending, concurrency, running, answered);

            std::ranges::for_each(answered, check);
            reported += answered.size();
            answered.clear();
            if (running.empty())
            {
                continue;
            }

            for (auto& snapshot : submitter.finishedQueries())
            {
                auto node = running.extract(snapshot.queryId);
                INVARIANT(not node.empty(), "a finished query was submitted by this run");
                auto& flight = node.mapped();

                const auto& statement = setUp.at(flight.job.setUpIndex).testCases.at(flight.job.testCaseIndex).at(flight.statement);
                const auto execution = extractExecutionTime(snapshot);
                const auto stopped = snapshot.getGlobalQueryStatus() == DistributedQueryStatus::Stopped;
                flight.outcomes.push_back(StatementOutcome{
                    .reached = std::move(snapshot),
                    .sinkOutputSchema = statement.plan.has_value() ? std::optional{statement.plan->sinkOutputSchema} : std::nullopt,
                    .explained = std::nullopt});
                flight.timings.push_back(StatementTiming{.execution = execution});
                ++flight.statement;

                /// Differential halves run in sequence.
                /// A failed first half skips the second: nothing to compare against.
                if (const auto started = stopped ? submitNext(submitter, flight) : std::nullopt)
                {
                    running.emplace(*started, std::move(flight));
                }
                else
                {
                    answered.push_back(std::move(flight));
                }
            }
        }
    }

    SystestBinder binder;
    SystestClusterConfiguration clusterConfig;
    bool remote;
    SingleNodeWorkerConfiguration baseWorker;

    void prepare(std::vector<RunnablePartition> partitions)
    {
        setUp.reserve(partitions.size());
        for (auto& partition : partitions)
        {
            const auto& [overrides, runnable] = partition;
            if (remote and not overrides.empty())
            {
                appendSkipped(
                    rejected, partition, "a run against workers started elsewhere cannot apply the settings that this file requires");
                continue;
            }
            try
            {
                auto [setupSql, staged] = stage(runnable);
                auto bound = binder.bind(setupSql, runnable.testCases, runnable.key);
                std::ranges::move(staged, std::back_inserter(servers));
                setUp.push_back(SetUpPartition{.partition = std::move(partition), .testCases = std::move(bound.testCases)});
            }
            catch (const std::exception& exception)
            {
                rejected.push_back(createFailedFileEntry(runnable.name, overrides, "could not run", exception));
                appendSkipped(rejected, partition, "the file's setup failed");
            }
        }
    }

    /// The partitions that preparation accepted, which every submit runs.
    std::vector<SetUpPartition> setUp;
    std::vector<ReportEntry> rejected;
    /// Must outlive every query reading from them.
    std::vector<std::jthread> servers;
};

TestRunner::TestRunner(const SystestConfiguration& config, std::vector<RunnablePartition> partitions) : impl{std::make_unique<Impl>(config)}
{
    impl->prepare(std::move(partitions));
}

TestRunner::~TestRunner() = default;

const std::vector<ReportEntry>& TestRunner::getRejected() const
{
    return impl->rejected;
}

size_t TestRunner::countTestCases() const
{
    return std::ranges::fold_left(
        impl->setUp, size_t{0}, [](const size_t sum, const Impl::SetUpPartition& setUp) { return sum + setUp.testCases.size(); });
}

std::vector<ReportEntry> TestRunner::submitAll(const size_t concurrency, const Observer& observe)
{
    struct Group
    {
        ConfigurationOverride overrides;
        std::deque<Impl::Job> jobs;
    };

    /// Slots follow file order, although the groups reorder the files.
    size_t slot = 0;
    std::vector<Group> groups;
    for (size_t setUpIndex = 0; setUpIndex < impl->setUp.size(); ++setUpIndex)
    {
        const auto& [overrides, file] = impl->setUp.at(setUpIndex).partition;
        const auto found = std::ranges::find(groups, overrides, &Group::overrides);
        auto& group = found == groups.end() ? groups.emplace_back(Group{.overrides = overrides, .jobs = {}}) : *found;
        for (size_t testCaseIndex = 0; testCaseIndex < file.testCases.size(); ++testCaseIndex)
        {
            group.jobs.push_back(Impl::Job{.setUpIndex = setUpIndex, .testCaseIndex = testCaseIndex, .reportSlot = slot++});
        }
    }

    std::vector<std::optional<ReportEntry>> checked(slot);
    for (auto& [overrides, jobs] : groups)
    {
        impl->submitGroup(overrides, std::move(jobs), concurrency, observe, checked);
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

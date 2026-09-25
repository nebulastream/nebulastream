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
#include <functional>
#include <iterator>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <fmt/format.h>

#include <Model/ConfigurationOverride.hpp>
#include <Model/RunnablePartition.hpp>
#include <Model/RunnableTestFile.hpp>
#include <Model/TestCaseId.hpp>
#include <Model/Verdict.hpp>
#include <QueryManager/EmbeddedWorkerQuerySubmissionBackend.hpp>
#include <QueryManager/GRPCQuerySubmissionBackend.hpp>
#include <QueryManager/QueryManager.hpp>
#include <ResultChecker/OutcomeChecker.hpp>
#include <Runner/QuerySubmitter.hpp>
#include <DistributedQuery.hpp>
#include <ErrorHandling.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <SystestBinder.hpp>
#include <WorkerCatalog.hpp>

namespace NES
{
namespace
{

/// A worker's starting configuration: what the command line gave, with the test file's settings applied on top.
SingleNodeWorkerConfiguration configuredWith(const SingleNodeWorkerConfiguration& base, const ConfigurationOverride& settings)
{
    auto configured = base;
    for (const auto& [key, value] : settings)
    {
        configured.overwriteConfigWithCommandLineInput({{key, value}});
    }
    return configured;
}

/// The span that the workers recorded between the query running and stopping.
/// Running rather than starting, so the time excludes bringing up sources and pipelines, as the measurements before this runner did.
/// A query that reported neither timestamp took no measurable time of its own.
std::chrono::milliseconds executionTime(const DistributedQueryStatusSnapshot& snapshot)
{
    const auto metrics = snapshot.coalesceQueryMetrics();
    if (not metrics.running.has_value() or not metrics.stop.has_value())
    {
        return std::chrono::milliseconds::zero();
    }
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(*metrics.stop - *metrics.running);
    return elapsed.count() > 0 ? elapsed : std::chrono::milliseconds::zero();
}

}

struct TestRunner::Impl
{
    explicit Impl(const SystestConfiguration& config)
        : binder(config)
        , clusterConfig(config.clusterConfig)
        , remote(config.remoteWorker.getValue())
        , baseWorker(config.singleNodeWorkerConfig.value_or(SingleNodeWorkerConfiguration{}))
    {
        if (not config.workerConfig.getValue().empty())
        {
            baseWorker.workerConfiguration.overwriteConfigWithYAMLFileInput(config.workerConfig.getValue());
        }
    }

    /// One test file partition that is set up: the settings that it runs under, and its test cases bound.
    struct Prepared
    {
        ConfigurationOverride settings;
        std::vector<std::vector<PlannedStatement>> testCases;
    };

    /// A submitter for these settings, which owns the worker that it submits to.
    /// One at a time: an embedded worker binds a receiver in this process, and the transport allows only one of those,
    /// so the group that runs under one set of settings finishes before the next group starts its worker.
    QuerySubmitter submitterFor(const ConfigurationOverride& settings)
    {
        auto catalog = std::make_shared<WorkerCatalog>(clusterConfig.workers);
        auto manager = remote
            ? std::make_unique<QueryManager>(std::move(catalog), createGRPCBackend())
            : std::make_unique<QueryManager>(std::move(catalog), createEmbeddedBackend(configuredWith(baseWorker, settings)));
        return QuerySubmitter{std::move(manager)};
    }

    /// One test case of one test file in the submitted group.
    struct Job
    {
        size_t runnable = 0;
        size_t index = 0;
    };

    /// One test case in flight: which of its statements is running, and what the statements answered so far.
    struct InFlight
    {
        Job job;
        size_t statement = 0;
        std::vector<StatementOutcome> outcomes;
        std::vector<QueryTiming> timings;
        std::chrono::steady_clock::time_point startedAt;
    };

    /// Runs the test files that asked for one set of settings, against the worker that has them.
    /// Submitting is asynchronous, so this starts up to `concurrency` test cases and then waits for whichever finishes
    /// first, rather than holding a thread per test case.
    void submitGroup(
        const std::vector<std::reference_wrapper<const RunnableTestFile>>& inGroup,
        const std::vector<size_t>& indices,
        const std::vector<size_t>& offsets,
        const ConfigurationOverride& settings,
        const size_t concurrency,
        const TestRunner::QueryObserver& observe,
        std::vector<ReportEntry>& checked)
    {
        auto submitter = submitterFor(settings);

        const auto statementsOf = [&](const Job& job) -> const std::vector<PlannedStatement>&
        { return prepared.at(indices.at(job.runnable)).testCases.at(job.index); };

        std::deque<Job> pending;
        for (size_t runnable = 0; runnable < inGroup.size(); ++runnable)
        {
            for (size_t index = 0; index < inGroup.at(runnable).get().testCases.size(); ++index)
            {
                pending.push_back(Job{.runnable = runnable, .index = index});
            }
        }
        const auto total = pending.size();
        std::unordered_map<DistributedQueryId, InFlight> running;
        std::deque<InFlight> answered;

        /// Submits the flight's next statement, or answers it here when it has no plan to run.
        /// Returns false once the test case has nothing further to submit, which is when it can be checked.
        const auto advance = [&](InFlight& flight)
        {
            const auto& statements = statementsOf(flight.job);
            if (flight.statement >= statements.size())
            {
                return false;
            }
            const auto& statement = statements.at(flight.statement);
            if (not statement.plan.has_value())
            {
                /// A statement that did not bind, and an EXPLAIN, are answered without reaching a worker.
                flight.outcomes.push_back(StatementOutcome{
                    .reached = std::unexpected{statement.plan.error()},
                    .sinkOutputSchema = std::nullopt,
                    .explained = statement.explained,
                    .execution = {}});
                flight.timings.emplace_back();
                ++flight.statement;
                return false;
            }
            auto started = submitter.startQuery(statement.plan->plan);
            if (not started.has_value())
            {
                flight.outcomes.push_back(StatementOutcome{
                    .reached = std::unexpected{started.error()},
                    .sinkOutputSchema = statement.plan->sinkOutputSchema,
                    .explained = std::nullopt,
                    .execution = {}});
                flight.timings.emplace_back();
                ++flight.statement;
                return false;
            }
            flight.startedAt = std::chrono::steady_clock::now();
            running.emplace(*started, std::move(flight));
            return true;
        };

        const auto check = [&](InFlight& flight)
        {
            const RunnableTestFile& runnable = inGroup.at(flight.job.runnable);
            const auto& testCase = runnable.testCases.at(flight.job.index);
            auto verdict = checkTestCase(flight.outcomes, testCase, runnable.originalNames);
            TestCaseId id{.originFile = runnable.name, .queryIdInFile = testCaseNumber(testCase), .overrides = settings};
            if (observe)
            {
                observe(id, testCase, verdict, flight.timings);
            }
            checked.at(offsets.at(indices.at(flight.job.runnable)) + flight.job.index)
                = ReportEntry{.id = std::move(id), .outcome = std::move(verdict), .timings = std::move(flight.timings)};
        };

        for (size_t reported = 0; reported < total;)
        {
            while (running.size() < std::max(concurrency, size_t{1}) and not pending.empty())
            {
                InFlight flight{.job = pending.front(), .statement = 0, .outcomes = {}, .timings = {}, .startedAt = {}};
                pending.pop_front();
                if (not advance(flight))
                {
                    answered.push_back(std::move(flight));
                }
            }

            while (not answered.empty())
            {
                auto flight = std::move(answered.front());
                answered.pop_front();
                check(flight);
                ++reported;
            }
            if (running.empty())
            {
                continue;
            }

            for (auto& snapshot : submitter.finishedQueries())
            {
                const auto inFlight = running.find(snapshot.queryId);
                INVARIANT(inFlight != running.end(), "a finished query was submitted by this run");
                auto flight = std::move(inFlight->second);
                running.erase(inFlight);

                const auto& statement = statementsOf(flight.job).at(flight.statement);
                const auto execution = executionTime(snapshot);
                const auto succeeded = snapshot.getGlobalQueryStatus() == DistributedQueryStatus::Stopped;
                flight.outcomes.push_back(StatementOutcome{
                    .reached = std::move(snapshot),
                    .sinkOutputSchema = statement.plan.has_value() ? std::optional{statement.plan->sinkOutputSchema} : std::nullopt,
                    .explained = std::nullopt,
                    .execution = execution});
                flight.timings.push_back(
                    QueryTiming{.submission = std::chrono::steady_clock::now() - flight.startedAt, .execution = execution});
                ++flight.statement;

                /// The halves of a differential block run one after the other, so the second reads a complete result file.
                /// A failed first half leaves the second unsubmitted, because its result would compare against nothing.
                if (not succeeded or not advance(flight))
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

    /// What setting up produced, in the order of the ready list that the caller then submits.
    std::vector<Prepared> prepared;
};

TestRunner::TestRunner(const SystestConfiguration& config) : impl(std::make_unique<Impl>(config))
{
}

TestRunner::~TestRunner() = default;

TestRunner::SetUpRun TestRunner::setUpAll(const std::vector<RunnablePartition>& partitions)
{
    SetUpRun run;
    run.ready.reserve(partitions.size());
    impl->prepared.clear();
    impl->prepared.reserve(partitions.size());

    for (const auto& [asked, runnable] : partitions)
    {
        /// Workers started elsewhere take no settings from this run, so a file that asks for some is skipped rather than failed,
        /// as the runner before this one did.
        if (impl->remote and not asked.empty())
        {
            for (const auto& testCase : runnable.testCases)
            {
                run.rejected.push_back(ReportEntry{
                    .id = TestCaseId{.originFile = runnable.name, .queryIdInFile = testCaseNumber(testCase), .overrides = asked},
                    .outcome
                    = Skipped{.reason = "a run against workers started elsewhere cannot apply the settings that this file asks for"},
                    .timings = {}});
            }
            continue;
        }
        /// Each file is set up on its own, so a failure reaches no further than that file.
        try
        {
            auto bound = impl->binder.bind(runnable);
            std::ranges::move(bound.servers, std::back_inserter(run.servers));
            impl->prepared.push_back(Impl::Prepared{.settings = asked, .testCases = std::move(bound.testCases)});
            run.ready.emplace_back(runnable);
        }
        catch (const std::exception& exception)
        {
            const std::string_view message{exception.what()};
            run.rejected.push_back(ReportEntry{
                .id = TestCaseId{.originFile = runnable.name, .queryIdInFile = std::nullopt, .overrides = asked},
                .outcome = Verdict{std::unexpected(Mismatch{
                    fmt::format("could not run: {}", message.empty() ? fmt::format("{}", getCurrentErrorCode()) : std::string{message})})},
                .timings = {}});
            /// The file's test cases are never submitted, and leaving them out would shrink the total with no trace,
            /// so each one reports that it was skipped.
            for (const auto& testCase : runnable.testCases)
            {
                run.rejected.push_back(ReportEntry{
                    .id = TestCaseId{.originFile = runnable.name, .queryIdInFile = testCaseNumber(testCase), .overrides = asked},
                    .outcome = Skipped{.reason = "the file's setup failed"},
                    .timings = {}});
            }
        }
    }
    return run;
}

std::vector<ReportEntry> TestRunner::submitQueries(
    const std::vector<std::reference_wrapper<const RunnableTestFile>>& runnables, const size_t concurrency, const QueryObserver& observe)
{
    INVARIANT(impl->prepared.size() == runnables.size(), "every test file that is ready to submit was set up");

    /// Where each test file's checks start in the report, so a test case keeps its place however the run groups the files.
    std::vector<size_t> offsets;
    offsets.reserve(runnables.size());
    size_t total = 0;
    for (const RunnableTestFile& runnable : runnables)
    {
        offsets.push_back(total);
        total += runnable.testCases.size();
    }
    std::vector<ReportEntry> checked(total);

    /// A worker takes its settings at startup, so the files are run one group of settings at a time, and each group
    /// waits for the group before it.
    /// Registering a worker per set of settings replaces this.
    std::vector<ConfigurationOverride> groups;
    for (const auto& prepared : impl->prepared)
    {
        if (std::ranges::find(groups, prepared.settings) == groups.end())
        {
            groups.push_back(prepared.settings);
        }
    }

    for (const auto& settings : groups)
    {
        std::vector<std::reference_wrapper<const RunnableTestFile>> inGroup;
        std::vector<size_t> indices;
        for (size_t index = 0; index < runnables.size(); ++index)
        {
            if (impl->prepared.at(index).settings == settings)
            {
                inGroup.emplace_back(runnables.at(index));
                indices.push_back(index);
            }
        }
        impl->submitGroup(inGroup, indices, offsets, settings, concurrency, observe, checked);
    }
    return checked;
}

std::vector<ReportEntry>
TestRunner::runAll(const std::vector<RunnablePartition>& partitions, const size_t concurrency, const QueryObserver& observe)
{
    auto run = setUpAll(partitions);
    std::ranges::move(submitQueries(run.ready, concurrency, observe), std::back_inserter(run.rejected));
    return std::move(run.rejected);
}

}

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
#include <functional>
#include <iterator>
#include <memory>
#include <span>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <fmt/format.h>

#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Model/ConfigurationOverride.hpp>
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

/// The span the workers recorded between the query starting and stopping.
/// A query that reported neither timestamp took no measurable time of its own.
std::chrono::milliseconds executionTime(const DistributedQueryStatusSnapshot& snapshot)
{
    const auto metrics = snapshot.coalesceQueryMetrics();
    if (not metrics.start.has_value() or not metrics.stop.has_value())
    {
        return std::chrono::milliseconds::zero();
    }
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(*metrics.stop - *metrics.start);
    return elapsed.count() > 0 ? elapsed : std::chrono::milliseconds::zero();
}

/// The time a finished query ran. A query that ran past the timeout reached no terminal state, so it took no measurable time.
std::chrono::milliseconds executionTimeOf(const FinishedQuery& finished)
{
    return finished.outcome.has_value() ? executionTime(*finished.outcome) : std::chrono::milliseconds::zero();
}

/// Whether a finished query ran to completion, as opposed to failing or running past the timeout.
bool completed(const FinishedQuery& finished)
{
    return finished.outcome.has_value() and finished.outcome->getGlobalQueryStatus() == DistributedQueryStatus::Stopped;
}

}

struct TestRunner::Impl
{
    explicit Impl(const SystestConfiguration& config)
        : binder(config)
        , clusterConfig(config.clusterConfig)
        , remote(config.remoteWorker.getValue())
        , baseWorker(config.singleNodeWorkerConfig.value_or(SingleNodeWorkerConfiguration{}))
        , queryTimeout(std::chrono::seconds{config.queryTimeoutSeconds.getValue()})
    {
        if (not config.workerConfig.getValue().empty())
        {
            baseWorker.workerConfiguration.overwriteConfigWithYAMLFileInput(config.workerConfig.getValue());
        }
    }

    /// One test file part that is set up: the settings it runs under, and its cases compiled.
    struct Prepared
    {
        ConfigurationOverride settings;
        std::vector<std::vector<PlannedStatement>> cases;
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
        return QuerySubmitter{std::move(manager), queryTimeout};
    }

    /// One case of one test file in the submitted group.
    struct Job
    {
        size_t runnable = 0;
        size_t index = 0;
    };

    /// One case in flight: which of its statements is running, and what the statements answered so far.
    struct InFlight
    {
        Job job;
        size_t statement = 0;
        std::vector<StatementOutcome> outcomes;
        std::vector<QueryTiming> timings;
        std::chrono::steady_clock::time_point startedAt;
    };

    /// Runs the test files that asked for one set of settings, against the worker that has them.
    /// Submitting is asynchronous, so this starts up to `concurrency` cases and then waits for whichever finishes
    /// first, rather than holding a thread per case.
    void submitGroup(
        const std::vector<std::reference_wrapper<const RunnableTestFile>>& inGroup,
        const std::vector<size_t>& indices,
        const std::vector<size_t>& offsets,
        const ConfigurationOverride& settings,
        const size_t concurrency,
        const TestRunner::QueryObserver& observe,
        std::vector<CheckedQuery>& checked)
    {
        auto submitter = submitterFor(settings);

        const auto statementsOf = [&](const Job& job) -> const std::vector<PlannedStatement>&
        { return prepared.at(indices.at(job.runnable)).cases.at(job.index); };

        std::deque<Job> pending;
        for (size_t runnable = 0; runnable < inGroup.size(); ++runnable)
        {
            for (size_t index = 0; index < inGroup.at(runnable).get().cases.size(); ++index)
            {
                pending.push_back(Job{.runnable = runnable, .index = index});
            }
        }
        const auto total = pending.size();
        std::unordered_map<DistributedQueryId, InFlight> running;
        std::deque<InFlight> answered;

        /// Submits the flight's next statement, or answers it here when it has no plan to run.
        /// Returns false once the case has nothing further to submit, which is when it can be checked.
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
                /// A statement that did not compile, and an EXPLAIN, are answered without reaching a worker.
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
            const auto& testCase = runnable.cases.at(flight.job.index);
            auto verdict = checkCase(flight.outcomes, testCase, runnable.qualifyingPrefix);
            TestCaseId id{.originFile = runnable.name, .queryIdInFile = caseNumber(testCase), .overrides = settings};
            if (observe)
            {
                observe(id, testCase, verdict, flight.timings);
            }
            checked.at(offsets.at(indices.at(flight.job.runnable)) + flight.job.index)
                = CheckedQuery{.id = std::move(id), .outcome = asOutcome(std::move(verdict)), .timings = std::move(flight.timings)};
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

            for (auto& finished : submitter.finishedQueries())
            {
                const auto inFlight = running.find(finished.id);
                INVARIANT(inFlight != running.end(), "a finished query was submitted by this run");
                auto flight = std::move(inFlight->second);
                running.erase(inFlight);

                const auto& statement = statementsOf(flight.job).at(flight.statement);
                const auto execution = executionTimeOf(finished);
                const auto succeeded = completed(finished);
                flight.outcomes.push_back(StatementOutcome{
                    .reached = std::move(finished.outcome),
                    .sinkOutputSchema = statement.plan.has_value() ? std::optional{statement.plan->sinkOutputSchema} : std::nullopt,
                    .explained = std::nullopt,
                    .execution = execution});
                flight.timings.push_back(
                    QueryTiming{.submission = std::chrono::steady_clock::now() - flight.startedAt, .execution = execution});
                ++flight.statement;

                /// The halves of a differential block run one after the other, so the second reads a result file that
                /// is complete. A failed first half leaves the second unsubmitted, because its result would compare
                /// against nothing.
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
    /// How long one submitted query may take to reach a terminal state. Zero waits forever.
    std::chrono::milliseconds queryTimeout;

    /// What setting up produced, in the order of the ready list that the caller then submits.
    std::vector<Prepared> prepared;
};

TestRunner::TestRunner(const SystestConfiguration& config) : impl(std::make_unique<Impl>(config))
{
}

TestRunner::~TestRunner() = default;

std::vector<RewrittenPart> TestRunner::rewrite(const DiscoveredTestFile& testfile)
{
    return impl->binder.rewrite(testfile);
}

TestRunner::SetUpRun
TestRunner::setUpAll(const std::vector<RunnableTestFile>& runnables, const std::span<const ConfigurationOverride> settings)
{
    INVARIANT(settings.size() == runnables.size(), "every test file states the settings it asks for");
    SetUpRun run;
    run.ready.reserve(runnables.size());
    impl->prepared.clear();
    impl->prepared.reserve(runnables.size());

    for (size_t index = 0; index < runnables.size(); ++index)
    {
        const auto& runnable = runnables.at(index);
        const auto& asked = settings[index];
        /// Each file is set up on its own, so a failure reaches no further than that file.
        try
        {
            if (impl->remote and not asked.empty())
            {
                throw TestException("a run against workers started elsewhere cannot apply the settings this file asks for");
            }
            auto compiled = impl->binder.plan(runnable);
            std::ranges::move(compiled.servers, std::back_inserter(run.servers));
            impl->prepared.push_back(Impl::Prepared{.settings = asked, .cases = std::move(compiled.cases)});
            run.ready.emplace_back(runnable);
        }
        catch (const std::exception& exception)
        {
            run.rejected.push_back(CheckedQuery{
                .id = TestCaseId{.originFile = runnable.name, .queryIdInFile = INVALID<SystestQueryId>, .overrides = asked},
                .outcome = Mismatch{fmt::format("could not run: {}", exception.what())},
                .timings = {}});
            /// The file's cases are never submitted, and leaving them out would shrink the total with no trace,
            /// so each one reports that it was skipped.
            for (const auto& testCase : runnable.cases)
            {
                run.rejected.push_back(CheckedQuery{
                    .id = TestCaseId{.originFile = runnable.name, .queryIdInFile = caseNumber(testCase), .overrides = asked},
                    .outcome = Skipped{.reason = "the file's setup failed"},
                    .timings = {}});
            }
        }
    }
    return run;
}

std::vector<CheckedQuery> TestRunner::submitQueries(
    const std::vector<std::reference_wrapper<const RunnableTestFile>>& runnables, const size_t concurrency, const QueryObserver& observe)
{
    INVARIANT(impl->prepared.size() == runnables.size(), "every test file that is ready to submit was set up");

    /// Where each test file's checks start in the report, so a case keeps its place however the run groups the files.
    std::vector<size_t> offsets;
    offsets.reserve(runnables.size());
    size_t total = 0;
    for (const RunnableTestFile& runnable : runnables)
    {
        offsets.push_back(total);
        total += runnable.cases.size();
    }
    std::vector<CheckedQuery> checked(total);

    /// A worker takes its settings at startup, so the files are run one group of settings at a time, and each group
    /// waits for the group before it. This is what registering a worker per set of settings replaces.
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

std::vector<CheckedQuery> TestRunner::runAll(
    const std::vector<RunnableTestFile>& runnables,
    const std::span<const ConfigurationOverride> settings,
    const size_t concurrency,
    const QueryObserver& observe)
{
    auto run = setUpAll(runnables, settings);
    std::ranges::move(submitQueries(run.ready, concurrency, observe), std::back_inserter(run.rejected));
    return std::move(run.rejected);
}

}

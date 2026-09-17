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
#include <cstdint>
#include <exception>
#include <filesystem>
#include <functional>
#include <iostream>
#include <iterator>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include <fmt/format.h>
#include <folly/MPMCQueue.h>
#include <nes-coordinator-bridge/coordinator.h>
#include <rfl/json/write.hpp>
#include <rust/cxx.h>
#include <BridgeError.hpp>

#include <Config/Config.hpp>
#include <Discovery/TestDiscovery.hpp>
#include <Discovery/TestFileReader.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Model/ConfigurationOverride.hpp>
#include <Model/ParsedTestFile.hpp>
#include <Model/RunnableTestFile.hpp>
#include <Model/TestCaseId.hpp>
#include <Model/Verdict.hpp>
#include <Parser/SystestParser.hpp>
#include <Parser/TestFileBuilder.hpp>
#include <Parser/TestFilePartition.hpp>
#include <ResultChecker/OutcomeChecker.hpp>
#include <Rewriter/NameQualifier.hpp>
#include <Rewriter/RewriteTarget.hpp>
#include <Rewriter/SourceRewriting.hpp>
#include <Rewriter/SqlRewriter.hpp>
#include <Runner/Cluster.hpp>
#include <Runner/DataStaging.hpp>
#include <Util/Overloaded.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{

/// Starts the coordinator with the optimizer settings the command line gave.
/// A separate function, so the JSON string outlives the call, which it would not if the member init list built it.
/// Empty settings pass nothing rather than an empty object, so the optimizer keeps its own defaults.
/// In the embedded mode the coordinator starts a worker in this process for each one registered with it.
/// In the remote mode it starts none and answers no default host, because it sends its commands over gRPC to the
/// worker already running at each registered address.
rust::Box<Bridge::Coordinator>
startCoordinator(const std::unordered_map<std::string, std::string>& optimizer, const Bridge::WorkerMode workers)
{
    if (optimizer.empty())
    {
        return Bridge::start_coordinator(rust::Str{}, workers, rust::Str{});
    }
    const auto json = rfl::json::write(optimizer);
    return Bridge::start_coordinator(rust::Str{}, workers, rust::Str{json.data(), json.size()});
}

Bridge::WorkerMode workerModeOf(const SystestConfiguration& config)
{
    return config.remoteWorker.getValue() ? Bridge::WorkerMode::Remote : Bridge::WorkerMode::Embedded;
}

/// One case of one test file in the submitted group, and the slot its check takes in the report.
struct Job
{
    size_t runnable = 0;
    size_t index = 0;
    size_t position = 0;
};

/// A case whose statements the coordinator has all answered: one for a query or an EXPLAIN, up to two for a differential block.
/// The submissions hold how long this process waited for each answer, in the same order as the outcomes.
struct Submitted
{
    Job job;
    std::vector<Bridge::StatementOutcome> outcomes;
    std::vector<std::chrono::steady_clock::duration> submissions;
};

/// The span the coordinator recorded between the query starting and stopping.
/// A statement that started no query reports neither timestamp, and takes no measurable time of its own.
std::chrono::milliseconds executionTime(const Bridge::StatementOutcome& outcome)
{
    return outcome.stop_ms > outcome.start_ms ? std::chrono::milliseconds{outcome.stop_ms - outcome.start_ms}
                                              : std::chrono::milliseconds::zero();
}

}

struct TestRunner::Impl
{
    explicit Impl(const SystestConfiguration& config)
        : workingDir(config.workingDir.getValue())
        , testDataDir(config.testDataDir.getValue())
        , configDir(config.configDir.getValue())
        , discoverRoot(config.testDiscoverRoot.getValue())
        /// An empty database path selects an in-memory catalog.
        , coordinator{startCoordinator(config.optimizerOverrides, workerModeOf(config))}
        , cluster{*coordinator, Cluster::Settings{.mode = workerModeOf(config), .topology = config.clusterConfig, .workerSettings = config.workerOverrides}}
        , queryTimeout{std::chrono::seconds{config.queryTimeoutSeconds.getValue()}}
    {
    }

    std::vector<RewrittenPart> rewrite(const DiscoveredTestFile& testfile)
    {
        SystestParser parser;
        parser.registerSubstitutionRule(
            {.keyword = "TESTDATA", .ruleFunction = [&](std::string& substitute) { substitute = testDataDir; }});
        parser.registerSubstitutionRule(
            {.keyword = "CONFIG/",
             .ruleFunction = [&](std::string& substitute)
             {
                 substitute = configDir;
                 if (!substitute.empty() && substitute.back() != '/')
                 {
                     substitute.push_back('/');
                 }
             }});
        parser.loadString(readTestFile(testfile.file));

        const ParsedTestFile parsedFile = [&]
        {
            try
            {
                return buildTestFile(parser, testfile.file);
            }
            catch (Exception& exception)
            {
                tryLogCurrentException();
                exception.what() += fmt::format("Could not successfully parse test file://{}", testfile.file.string());
                throw;
            }
        }();

        const DiscoveryRoot discoveryRoot{discoverRoot};
        auto parts = partitionByOverrides(parsedFile);
        const auto selected = testfile.enabledQueries.value_or(std::unordered_set<SystestQueryId>{});

        std::vector<RewrittenPart> rewritten;
        rewritten.reserve(parts.size());
        std::unordered_set<SystestQueryId> foundQueries;
        for (size_t index = 0; index < parts.size(); ++index)
        {
            auto& [overrides, file] = parts.at(index);
            /// The rewriter needs the hosts before it emits SQL, and asking for them registers the worker for these settings.
            const auto placement = cluster.placementFor(overrides);
            if (not placement.has_value())
            {
                fmt::print("Skipping {} because it asks for worker settings the run cannot give it\n", testfile.name().getRawValue());
                continue;
            }
            const auto partKey = discoveryRoot.keyOf(testfile.file, index, parts.size());
            /// The rewrite consumes the part's statements. Only its overrides are read afterwards.
            auto runnable = rewriteTestFile(
                std::move(file),
                RewriteTarget{
                    .testFileKey = partKey,
                    .displayName = testfile.name().getRawValue(),
                    .workingDir = workingDir,
                    .testDataDir = testDataDir,
                    .sourceHost = placement->sources,
                    .sinkHost = placement->sinks});
            runnable.key = partKey.value();
            for (const auto& testCase : runnable.cases)
            {
                /// Both numbers of a differential block select it, so both count as found.
                foundQueries.insert(caseNumber(testCase));
                if (const auto* differential = std::get_if<RewrittenDifferential>(&testCase.action))
                {
                    foundQueries.insert(differential->secondId);
                }
            }
            keepSelectedCases(runnable, selected);
            /// When the selection drops every case of a part, nothing runs, so its data is not staged.
            if (runnable.cases.empty())
            {
                continue;
            }
            rewritten.push_back(RewrittenPart{.settings = overrides, .test = std::move(runnable)});
        }

        for (const auto badTestNumber : selected)
        {
            if (not foundQueries.contains(badTestNumber))
            {
                std::cerr << fmt::format(
                    "Warning: Query number {} specified via command line argument but not found in file://{}",
                    badTestNumber,
                    testfile.file.string());
            }
        }
        return rewritten;
    }

    /// Stages the data the sources read, then submits the setup statements of one test file.
    /// Returns the servers that are still sending, which have to outlive every query reading from them.
    /// Throws on the first statement the coordinator rejects, leaving the rest of that file's setup unsubmitted.
    /// The servers it had already started stop as the throw unwinds, because nothing will read from them.
    [[nodiscard]] std::vector<std::jthread> submitSetup(const RunnableTestFile& runnable)
    {
        std::vector<std::jthread> servers;
        /// The staged SQL is what reaches the coordinator, so a served source includes the endpoint that its server bound.
        for (auto setup : runnable.setupStatements)
        {
            std::visit(
                Overloaded{
                    [](const PlainStatement&) {},
                    [](const StatementWithInlineData& withInline) { writeInlineData(withInline.data); },
                    [&](StatementWithServedData& withServed)
                    {
                        /// The server binds a port on the host running this process and advertises it as `localhost`.
                        /// A worker in another process resolves that to itself and connects somewhere unrelated, so
                        /// reporting the file is better than letting it read whatever answers.
                        /// The distributed harness excludes the `tcp` group for the same reason.
                        if (not cluster.runsInThisProcess())
                        {
                            throw TestException("a source served over a socket cannot reach a remote worker: exclude the tcp group");
                        }
                        auto server = serve(std::move(withServed.data));
                        withServed.sql = addSourceOptions(withServed.sql, server.options);
                        servers.push_back(std::move(server.thread));
                    }},
                setup);
            /// A CREATE cannot state an expected error, because only a query can, so a rejected one is a broken test file.
            /// Throwing skips the rest of this file's setup, since a statement below usually needs the one that failed.
            /// What is already in the catalog stays there, which costs nothing: this file's queries never run, and its names
            /// carry its key so no other file can reach them.
            const auto& sql = sqlOf(setup);
            if (const auto outcome
                = coordinator->submit(rust::Str{sql.data(), sql.size()}, Bridge::WaitMode::UntilCompleted, timeoutMillis(), false);
                not Bridge::isNone(outcome.error))
            {
                throw TestException("setup statement failed: {}: {}", sql, std::string{outcome.error.msg});
            }
        }
        return servers;
    }

    /// The deadline for one submission, in the unit the bridge takes.
    [[nodiscard]] uint64_t timeoutMillis() const { return static_cast<uint64_t>(queryTimeout.count()); }

    std::filesystem::path workingDir;
    std::filesystem::path testDataDir;
    std::filesystem::path configDir;
    std::filesystem::path discoverRoot;

    rust::Box<Bridge::Coordinator> coordinator;

    /// Declared after the coordinator, because registering the workers and reading the default host both need one.
    Cluster cluster;

    /// How long one submission waits before the coordinator gives up on it. Zero waits forever.
    std::chrono::milliseconds queryTimeout;

    /// The settings of each test file that is ready to submit, in the order of the ready list, which label its checks.
    std::vector<ConfigurationOverride> prepared;
};

TestRunner::TestRunner(const SystestConfiguration& config) : impl(std::make_unique<Impl>(config))
{
}

TestRunner::~TestRunner() = default;

std::vector<RewrittenPart> TestRunner::rewrite(const DiscoveredTestFile& testfile)
{
    return impl->rewrite(testfile);
}

std::optional<Placement> TestRunner::placementFor(const ConfigurationOverride& settings)
{
    return impl->cluster.placementFor(settings);
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
            std::ranges::move(impl->submitSetup(runnable), std::back_inserter(run.servers));
            impl->prepared.push_back(asked);
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

    /// Every case of every test file, with the slot its check takes in the report, so a case keeps its place however
    /// the run interleaves them.
    std::vector<Job> jobs;
    for (size_t runnable = 0; runnable < runnables.size(); ++runnable)
    {
        for (size_t index = 0; index < runnables.at(runnable).get().cases.size(); ++index)
        {
            jobs.push_back(Job{.runnable = runnable, .index = index, .position = jobs.size()});
        }
    }

    /// Each queue holds a fixed number of slots, so neither grows without a limit.
    /// The run writes every job once, plus one empty job per thread, which sizes the first.
    /// A thread takes its next job only after handing its answer over, so at most one answer per thread waits, which sizes the second.
    const auto threads = std::max(concurrency, size_t{1});
    folly::MPMCQueue<std::optional<Job>> toSubmit{jobs.size() + threads};
    folly::MPMCQueue<Submitted> answered{threads};

    /// One thread per concurrent case, because a submission blocks until its statement is terminal.
    /// Each thread takes a job, submits its statements and hands the answers back, so at most this many cases run at once.
    /// The coordinator serves each submission on a request of its own, so several threads may wait inside one at the same time.
    /// An empty job releases a thread, and the loop below writes one per thread once every answer is in.
    const auto nextJob = [&toSubmit]
    {
        std::optional<Job> job;
        toSubmit.blockingRead(job);
        return job;
    };
    const auto submitOne = [&](const std::string& sql, Submitted& answer)
    {
        const auto startedAt = std::chrono::steady_clock::now();
        answer.outcomes.push_back(
            impl->coordinator->submit(rust::Str{sql.data(), sql.size()}, Bridge::WaitMode::UntilCompleted, impl->timeoutMillis(), false));
        answer.submissions.push_back(std::chrono::steady_clock::now() - startedAt);
    };
    const auto submit = [&]
    {
        while (const auto job = nextJob())
        {
            const RunnableTestFile& runnable = runnables.at(job->runnable);
            Submitted answer{.job = *job, .outcomes = {}, .submissions = {}};
            std::visit(
                Overloaded{
                    [&](const RewrittenQuery& query) { submitOne(query.sql, answer); },
                    [&](const RewrittenDifferential& block)
                    {
                        /// The halves run one after the other on this thread, so a block takes one slot for its whole
                        /// span and the second half reads a result file that is complete.
                        /// A failed first half leaves the second unsubmitted, because its result would compare against nothing.
                        submitOne(block.firstSql, answer);
                        if (Bridge::isNone(answer.outcomes.back().error))
                        {
                            submitOne(block.secondSql, answer);
                        }
                    },
                    [&](const RewrittenExplain& explain) { submitOne(explain.sql, answer); }},
                runnable.cases.at(job->index).action);
            answered.blockingWrite(std::move(answer));
        }
    };
    std::vector<std::jthread> submitters;
    submitters.reserve(threads);
    while (submitters.size() < threads)
    {
        submitters.emplace_back(submit);
    }

    /// Checking stays on this thread, so the checks and the observer need no locking.
    /// The threads above spend the time, because running a query costs far more than comparing its result.
    /// Writing a job never blocks, because the queue has a slot for every one, so a thread waiting to hand an answer over
    /// is always let through.
    std::vector<CheckedQuery> checked(jobs.size());
    for (const auto& job : jobs)
    {
        toSubmit.blockingWrite(job);
    }
    for (size_t reported = 0; reported < jobs.size(); ++reported)
    {
        Submitted answer;
        answered.blockingRead(answer);
        const auto& [job, outcomes, submissions] = answer;
        const RunnableTestFile& runnable = runnables.at(job.runnable);
        const auto& testCase = runnable.cases.at(job.index);
        auto verdict = checkCase(outcomes, testCase, runnable.qualifyingPrefix);
        std::vector<QueryTiming> timings;
        timings.reserve(outcomes.size());
        for (size_t statement = 0; statement < outcomes.size(); ++statement)
        {
            timings.push_back(QueryTiming{.submission = submissions.at(statement), .execution = executionTime(outcomes.at(statement))});
        }
        TestCaseId id{.originFile = runnable.name, .queryIdInFile = caseNumber(testCase), .overrides = impl->prepared.at(job.runnable)};
        if (observe)
        {
            observe(id, testCase, verdict, timings);
        }
        checked.at(job.position)
            = CheckedQuery{.id = std::move(id), .outcome = asOutcome(std::move(verdict)), .timings = std::move(timings)};
    }
    std::ranges::for_each(submitters, [&](const auto&) { toSubmit.blockingWrite(std::nullopt); });
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

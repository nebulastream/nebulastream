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
#include <exception>
#include <expected>
#include <functional>
#include <iterator>
#include <optional>
#include <ranges>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include <coordinator/lib.h>
#include <fmt/format.h>
#include <folly/MPMCQueue.h>
#include <rfl/json/write.hpp>
#include <rust/cxx.h>

#include <Model/RunnableTest.hpp>
#include <Model/SystestQueryId.hpp>
#include <Model/TestCaseId.hpp>
#include <Model/Verdict.hpp>
#include <ResultChecker/OutcomeChecker.hpp>
#include <Rewriter/SqlRewriter.hpp>
#include <Runner/DataStaging.hpp>
#include <Runner/Schedule.hpp>
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
rust::Box<EmbeddedCoordinator> startCoordinator(const std::unordered_map<std::string, std::string>& optimizer, const WorkerMode workers)
{
    if (optimizer.empty())
    {
        return start_embedded_coordinator(rust::Str{}, workers, rust::Str{});
    }
    const auto json = rfl::json::write(optimizer);
    return start_embedded_coordinator(rust::Str{}, workers, rust::Str{json.data(), json.size()});
}

/// A case whose statements the coordinator has all answered: one for a query, up to two for a differential block.
/// The submissions hold how long this process waited for each answer, in the same order as the outcomes.
struct Submitted
{
    Job job;
    std::vector<StatementOutcome> outcomes;
    std::vector<std::chrono::steady_clock::duration> submissions;
};

/// The span the coordinator recorded between the query starting and stopping.
/// A statement that started no query reports neither timestamp, and takes no measurable time of its own.
std::chrono::milliseconds executionTime(const StatementOutcome& outcome)
{
    return outcome.stop_ms > outcome.start_ms ? std::chrono::milliseconds{outcome.stop_ms - outcome.start_ms}
                                              : std::chrono::milliseconds::zero();
}

}

TestRunner::TestRunner(const RunSettings& settings)
    /// An empty database path selects an in-memory catalog.
    : coordinator{startCoordinator(settings.optimizer, settings.cluster.mode)}
    , cluster{*coordinator, settings.cluster}
    , queryTimeout{settings.queryTimeout}
{
}

std::vector<std::jthread> TestRunner::submitSetup(const RunnableTest& runnable)
{
    std::vector<std::jthread> servers;
    for (auto [sql, stagedData] : runnable.createStmts)
    {
        if (stagedData.has_value())
        {
            std::visit(
                Overloaded{
                    [](const InlineData& inlineData) { writeInlineData(inlineData); },
                    [&](const ServedData& servedData)
                    {
                        /// The server binds a port on the host running this process and advertises it as `localhost`.
                        /// A worker in another process resolves that to itself and connects somewhere unrelated, so
                        /// reporting the file is better than letting it read whatever answers.
                        /// The distributed harness excludes the `tcp` group for the same reason.
                        if (not cluster.runsInThisProcess())
                        {
                            throw TestException("a source served over a socket cannot reach a remote worker: exclude the tcp group");
                        }
                        auto [thread, options] = serve(servedData);
                        sql = addSourceOptions(sql, options);
                        servers.push_back(std::move(thread));
                    }},
                *stagedData);
        }
        /// A CREATE cannot state an expected error, because only a query can, so a rejected one is a broken test file.
        /// Throwing skips the rest of this file's setup, since a statement below usually needs the one that failed.
        /// What is already in the catalog stays there, which costs nothing: this file's queries never run, and its names
        /// carry its key so no other file can reach them.
        if (const auto outcome = coordinator->submit(rust::Str{sql.data(), sql.size()}, timeoutMillis()); outcome.error.code != 0)
        {
            throw TestException("setup statement failed: {}: {}", sql, std::string{outcome.error.msg});
        }
    }
    return servers;
}

TestRunner::SetUpRun TestRunner::setUpAll(const std::vector<RunnableTest>& runnables)
{
    SetUpRun run;
    run.ready.reserve(runnables.size());
    for (const auto& runnable : runnables)
    {
        /// Each file gets its own setup call, so a failure reaches no further than that file.
        /// The throw leaves the rest of that file's setup unsubmitted and its queries out of the schedule, and this loop
        /// goes on to the next file.
        try
        {
            std::ranges::move(submitSetup(runnable), std::back_inserter(run.servers));
            run.ready.emplace_back(runnable);
        }
        catch (const std::exception& exception)
        {
            run.rejected.push_back(CheckedQuery{
                .id = TestCaseId{.file = runnable.name, .variant = runnable.variant, .query = INVALID<SystestQueryId>},
                .outcome = Mismatch{fmt::format("could not run: {}", exception.what())},
                .timings = {}});
            /// The file's cases never enter the schedule, and leaving them out would shrink the total with no trace,
            /// so each one reports that it was skipped.
            for (const auto& testCase : runnable.cases)
            {
                run.rejected.push_back(CheckedQuery{
                    .id = TestCaseId{.file = runnable.name, .variant = runnable.variant, .query = caseNumber(testCase)},
                    .outcome = Skipped{.reason = "the file's setup failed"},
                    .timings = {}});
            }
        }
    }
    return run;
}

std::vector<CheckedQuery>
TestRunner::runAll(const std::vector<RunnableTest>& runnables, const size_t concurrency, const QueryObserver& observe)
{
    auto run = setUpAll(runnables);
    std::ranges::move(submitQueries(run.ready, concurrency, observe), std::back_inserter(run.rejected));
    return std::move(run.rejected);
}

std::vector<CheckedQuery> TestRunner::submitQueries(
    const std::vector<std::reference_wrapper<const RunnableTest>>& runnables, const size_t concurrency, const QueryObserver& observe)
{
    Schedule schedule{runnables};

    /// Each queue holds a fixed number of slots, so neither grows without a limit.
    /// The run writes every job once, plus one empty job per thread, which sizes the first.
    /// A thread takes its next job only after handing its answer over, so at most one answer per thread waits, which sizes the second.
    folly::MPMCQueue<std::optional<Job>> toSubmit{schedule.size() + concurrency};
    folly::MPMCQueue<Submitted> answered{concurrency};

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
        answer.outcomes.push_back(coordinator->submit(rust::Str{sql.data(), sql.size()}, timeoutMillis()));
        answer.submissions.push_back(std::chrono::steady_clock::now() - startedAt);
    };
    const auto submit = [&]
    {
        while (const auto job = nextJob())
        {
            const RunnableTest& runnable = runnables.at(job->runnable);
            Submitted answer{.job = *job, .outcomes = {}, .submissions = {}};
            std::visit(
                Overloaded{
                    [&](const RunnableQuery& query) { submitOne(query.sql, answer); },
                    [&](const RunnableDifferential& differential)
                    {
                        /// The halves run one after the other on this thread, so a block takes one slot for its whole
                        /// span and the second half reads a result file that is complete.
                        /// A failed first half leaves the second unsubmitted, because its result would compare against nothing.
                        submitOne(differential.firstSql, answer);
                        if (answer.outcomes.back().error.code == 0)
                        {
                            submitOne(differential.secondSql, answer);
                        }
                    }},
                runnable.cases.at(job->index).action);
            answered.blockingWrite(std::move(answer));
        }
    };
    std::vector<std::jthread> submitters;
    submitters.reserve(concurrency);
    while (submitters.size() < concurrency)
    {
        submitters.emplace_back(submit);
    }

    /// Checking stays on this thread, so the checks, the ordering rules and the observer need no locking.
    /// The threads above spend the time, because running a query costs far more than comparing its result.
    /// Writing a job never blocks, because the queue has a slot for every one, so a thread waiting to hand an answer over
    /// is always let through.
    std::vector<CheckedQuery> checked(schedule.size());
    for (const auto& job : schedule.ready())
    {
        toSubmit.blockingWrite(job);
    }
    for (size_t reported = 0; reported < schedule.size(); ++reported)
    {
        Submitted answer;
        answered.blockingRead(answer);
        const auto& [job, outcomes, submissions] = answer;
        const RunnableTest& runnable = runnables.at(job.runnable);
        const auto& testCase = runnable.cases.at(job.index);
        auto verdict = checkCase(outcomes, testCase);
        std::vector<QueryTiming> timings;
        timings.reserve(outcomes.size());
        for (size_t statement = 0; statement < outcomes.size(); ++statement)
        {
            timings.push_back(QueryTiming{.submission = submissions.at(statement), .execution = executionTime(outcomes.at(statement))});
        }
        TestCaseId id{.file = runnable.name, .variant = runnable.variant, .query = caseNumber(testCase)};
        if (observe)
        {
            observe(id, testCase, verdict, timings);
        }
        checked.at(job.position)
            = CheckedQuery{.id = std::move(id), .outcome = asOutcome(std::move(verdict)), .timings = std::move(timings)};
        for (const auto& released : schedule.completed(job))
        {
            toSubmit.blockingWrite(released);
        }
    }
    std::ranges::for_each(submitters, [&](const auto&) { toSubmit.blockingWrite(std::nullopt); });
    return checked;
}

}

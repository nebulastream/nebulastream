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

#include <Executor.hpp>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <expected>
#include <functional>
#include <iterator>
#include <numeric>
#include <optional>
#include <random>
#include <span>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include <fmt/base.h>
#include <fmt/format.h>
#include <fmt/ranges.h>

#include <Config/RunPolicy.hpp>
#include <Discovery/TestDiscovery.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Model/ConfigurationOverride.hpp>
#include <Model/RunnableTestFile.hpp>
#include <Model/TestCaseId.hpp>
#include <Model/Verdict.hpp>
#include <Runner/TestRunner.hpp>
#include <Util/Overloaded.hpp>
#include <Benchmark.hpp>
#include <ErrorHandling.hpp>
#include <Logging.hpp>
#include <Progress.hpp>
#include <WorkingDirectoryGuard.hpp>

/// Switches the workers to in-memory communication, which the systest run needs because it starts its workers in this
/// process: the transport otherwise binds one receiver socket per process and a second worker cannot come up.
extern void enable_memcom();

namespace NES
{
namespace
{

/// Prints one checked case as it finishes, so a long run shows what it is doing rather than only its tally.
void printProgress(SystestProgressTracker& progress, const TestCaseId& id, const Verdict& verdict)
{
    progress.incrementQueryCounter();
    const auto label = fmt::format("{}", id);
    fmt::print(
        "[{}/{}] {:.<100} {}\n", progress.getQueryCounter(), progress.getTotalQueries(), label, verdict.has_value() ? "PASSED" : "FAILED");
    if (not verdict.has_value())
    {
        fmt::print("{}\n", verdict.error().detail);
    }
    static_cast<void>(std::fflush(stdout));
}

/// How many cases the prepared test files hold, which is how many checks the run produces.
size_t caseCount(const std::vector<RunnableTestFile>& prepared)
{
    size_t total = 0;
    for (const auto& file : prepared)
    {
        total += file.cases.size();
    }
    return total;
}

/// How many rounds the repetition asks for. Zero stands for no round limit, which only the time limit or a failure ends.
uint64_t roundsOf(const RepetitionPolicy& repetition)
{
    if (const auto* fixed = std::get_if<SubmitRounds>(&repetition))
    {
        return fixed->count;
    }
    return std::holds_alternative<SubmitUntilStopped>(repetition) ? 0 : 1;
}

/// Records what one round measured, reading the checks and the cases side by side: the runner reports in file order,
/// so the check at each position belongs to the case at that position.
/// A case that failed measured nothing, and the failure ends the run after its round anyway.
/// A differential block has no input files and is not measured. A measuring run skipped it before the rewriter existed too.
/// An EXPLAIN is answered while compiling and never runs, so there is no execution to measure.
void recordRound(
    Benchmark& benchmark,
    const std::vector<std::reference_wrapper<const RunnableTestFile>>& files,
    const std::vector<CheckedQuery>& checked)
{
    size_t position = 0;
    for (const RunnableTestFile& file : files)
    {
        for (const auto& testCase : file.cases)
        {
            const auto& result = checked.at(position++);
            if (not std::holds_alternative<Success>(result.outcome) or result.timings.empty())
            {
                continue;
            }
            std::visit(
                Overloaded{
                    [&](const RewrittenQuery& query)
                    {
                        benchmark.record(
                            fmt::format("{}:{}", file.name, query.id.getRawValue()), query.inputFiles, result.timings.front().execution);
                    },
                    [](const RewrittenDifferential&) {},
                    [](const RewrittenExplain&) {}},
                testCase.action);
        }
    }
}

}

Executor::Executor(SystestConfiguration config) : config(std::move(config))
{
}

std::expected<std::vector<RunnableTestFile>, CheckedQuery>
Executor::prepare(const DiscoveredTestFile& discovered, TestRunner& runner, std::vector<ConfigurationOverride>& settings)
{
    const auto name = discovered.name();
    try
    {
        std::vector<RunnableTestFile> prepared;
        for (auto& part : runner.rewrite(discovered))
        {
            settings.push_back(std::move(part.settings));
            prepared.push_back(std::move(part.test));
        }
        return prepared;
    }
    catch (const Exception& exception)
    {
        const std::string message{exception.what()};
        return std::unexpected(CheckedQuery{
            .id = TestCaseId{.originFile = name.getRawValue(), .queryIdInFile = INVALID<SystestQueryId>, .overrides = {}},
            .outcome = Mismatch{fmt::format("could not prepare: {}", message.empty() ? fmt::format("{}", exception.code()) : message)},
            .timings = {}});
    }
    catch (const std::exception& exception)
    {
        return std::unexpected(CheckedQuery{
            .id = TestCaseId{.originFile = name.getRawValue(), .queryIdInFile = INVALID<SystestQueryId>, .overrides = {}},
            .outcome = Mismatch{fmt::format("could not prepare: {}", exception.what())},
            .timings = {}});
    }
}

Executor::PreparedRun Executor::prepareAll(TestRunner& runner) const
{
    PreparedRun run;
    for (const auto& discovered : discoverTestFiles(config))
    {
        fmt::print("Loading queries from test file: file://{}\n", discovered.getLogFilePath());
        if (auto ready = prepare(discovered, runner, run.settings))
        {
            std::ranges::move(*ready, std::back_inserter(run.ready));
        }
        else
        {
            run.unprepared.push_back(std::move(ready.error()));
        }
    }
    return run;
}

ExecutorResult Executor::summarize(const std::vector<CheckedQuery>& checked)
{
    /// An invocation that ran no query has not passed. It has not run.
    /// Reporting success would let a group name that nobody kept up to date, or a filter that matches nothing, go unnoticed.
    if (checked.empty())
    {
        return RunFailed{
            .report = "no query ran: the test location, the groups and the disable config select nothing\n",
            .errorCode = ErrorCode::TestException};
    }

    /// One line per skipped file rather than one per skipped case, because every case of a file skips for the same reason.
    struct SkipLine
    {
        std::string label;
        std::string reason;
        size_t count = 0;
    };

    std::vector<std::string> failures;
    std::vector<SkipLine> skips;
    for (const auto& query : checked)
    {
        std::visit(
            Overloaded{
                [](const Success&) {},
                [&](const Mismatch& mismatch) { failures.push_back(fmt::format("  FAIL  {}: {}\n", query.id, mismatch.detail)); },
                [&](const Skipped& skip)
                {
                    /// The label drops the query number, so every case of one file part folds into one line.
                    auto label = fmt::format(
                        "{}",
                        TestCaseId{
                            .originFile = query.id.originFile, .queryIdInFile = INVALID<SystestQueryId>, .overrides = query.id.overrides});
                    if (not skips.empty() and skips.back().label == label and skips.back().reason == skip.reason)
                    {
                        ++skips.back().count;
                        return;
                    }
                    skips.push_back(SkipLine{.label = std::move(label), .reason = skip.reason, .count = 1});
                }},
            query.outcome);
    }

    std::string details = fmt::to_string(fmt::join(failures, ""));
    size_t skipped = 0;
    for (const auto& [label, reason, count] : skips)
    {
        skipped += count;
        details += fmt::format("  SKIP  {}: {} cases: {}\n", label, count, reason);
    }

    const auto passed = checked.size() - failures.size() - skipped;
    /// The skip tally appears only when something skipped, so the everyday report reads as before.
    const auto headline = skipped > 0 ? fmt::format("{} queries passed, {} failed, {} skipped\n", passed, failures.size(), skipped)
                                      : fmt::format("{} queries passed, {} failed\n", passed, failures.size());
    if (failures.empty())
    {
        return RunSucceeded{.report = headline + details};
    }
    return RunFailed{.report = headline + details, .errorCode = ErrorCode::TestException};
}

ExecutorResult Executor::runOnce(TestRunner& runner, const RunPolicy& plan, PreparedRun prepared)
{
    SystestProgressTracker progress{caseCount(prepared.ready)};

    /// What failed before any query ran joins the report next to the cases that run below.
    auto setUp = runner.setUpAll(prepared.ready, prepared.settings);
    std::vector<CheckedQuery> report = std::move(prepared.unprepared);
    std::ranges::move(setUp.rejected, std::back_inserter(report));

    const TestRunner::QueryObserver observe
        = [&](const TestCaseId& id, const RewrittenCase&, const Verdict& verdict, const std::span<const QueryTiming>)
    { printProgress(progress, id, verdict); };

    std::ranges::move(runner.submitQueries(setUp.ready, plan.concurrency, observe), std::back_inserter(report));
    return summarize(report);
}

ExecutorResult Executor::runRounds(TestRunner& runner, const RunPolicy& plan, PreparedRun prepared)
{
    /// A repeating run measures what it runs or puts a worker under load, and a file that could not be prepared or set
    /// up is a failure of the invocation rather than something to repeat.
    if (not prepared.unprepared.empty())
    {
        return summarize(prepared.unprepared);
    }
    auto setUp = runner.setUpAll(prepared.ready, prepared.settings);
    if (not setUp.rejected.empty())
    {
        return summarize(setUp.rejected);
    }

    /// Filled only when the run measures, which is the only difference to a load run: each round then records how long
    /// every passing query took, and the run ends in the written report rather than a tally.
    const bool measuring = plan.measureReport.has_value();
    Benchmark benchmark;

    const auto rounds = roundsOf(plan.repetition);
    if (not measuring)
    {
        fmt::print("Repeating the queries of {} test files\n", setUp.ready.size());
    }

    const auto startedAt = std::chrono::steady_clock::now();
    for (uint64_t round = 1; rounds == 0 or round <= rounds; ++round)
    {
        const auto roundStartedAt = std::chrono::steady_clock::now();
        const auto checked = runner.submitQueries(setUp.ready, plan.concurrency);
        const auto failed
            = std::ranges::count_if(checked, [](const CheckedQuery& query) { return not std::holds_alternative<Success>(query.outcome); });
        if (measuring)
        {
            recordRound(benchmark, setUp.ready, checked);
            fmt::print("round {} of {} measured\n", round, rounds);
        }
        else
        {
            fmt::print(
                "round {}: {} passed, {} failed in {} ms\n",
                round,
                checked.size() - static_cast<size_t>(failed),
                failed,
                std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - roundStartedAt).count());
        }
        static_cast<void>(std::fflush(stdout));

        /// A wrong result ends the run, because the rounds after it would measure or load the same wrong query.
        if (failed > 0)
        {
            return summarize(checked);
        }
        if (plan.runLimit.has_value() and std::chrono::steady_clock::now() - startedAt >= *plan.runLimit)
        {
            break;
        }
    }

    if (plan.measureReport.has_value())
    {
        return RunSucceeded{.report = benchmark.writeTo(*plan.measureReport)};
    }
    return RunSucceeded{.report = "every round passed\n"};
}

ExecutorResult Executor::execute() const
{
    setupLogging(config);
    const auto plan = RunPolicy::create(config);
    const WorkingDirectoryGuard workingDirectoryGuard{config.workingDir.getValue()};

    if (not config.remoteWorker.getValue())
    {
        enable_memcom();
    }

    TestRunner runner{config};
    auto prepared = prepareAll(runner);

    if (const auto* shuffle = std::get_if<RunInShuffledOrder>(&plan.ordering))
    {
        /// The seed is printed, so a failure this finds can be repeated with the same seed.
        /// That repeats the order the files are submitted in, not the order their queries finish, which the pool decides.
        const uint64_t seed = shuffle->seed.has_value() ? *shuffle->seed : std::random_device{}();
        fmt::print("Running {} test files in random order, with seed {}\n", prepared.ready.size(), seed);

        /// The settings stay with their test file, so both move together.
        std::vector<size_t> order(prepared.ready.size());
        std::ranges::iota(order, size_t{0});
        std::ranges::shuffle(order, std::mt19937_64{seed});
        std::vector<RunnableTestFile> shuffled;
        std::vector<ConfigurationOverride> shuffledSettings;
        shuffled.reserve(order.size());
        shuffledSettings.reserve(order.size());
        for (const auto index : order)
        {
            shuffled.push_back(std::move(prepared.ready.at(index)));
            shuffledSettings.push_back(std::move(prepared.settings.at(index)));
        }
        prepared.ready = std::move(shuffled);
        prepared.settings = std::move(shuffledSettings);
    }

    if (std::holds_alternative<SubmitOnce>(plan.repetition))
    {
        return runOnce(runner, plan, std::move(prepared));
    }
    return runRounds(runner, plan, std::move(prepared));
}

}

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
#include <map>
#include <optional>
#include <random>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <Config/RunPolicy.hpp>
#include <Discovery/TestDiscovery.hpp>
#include <Model/ConfigurationOverride.hpp>
#include <Model/Expectation.hpp>
#include <Model/RunnablePartition.hpp>
#include <Model/RunnableTestFile.hpp>
#include <Model/TestCaseId.hpp>
#include <Model/Verdict.hpp>
#include <Rewriter/TestFileRewriter.hpp>
#include <Runner/TestRunner.hpp>
#include <Benchmark.hpp>
#include <ErrorHandling.hpp>
#include <Logging.hpp>
#include <Progress.hpp>
#include <WorkingDirectoryGuard.hpp>

namespace NES
{
namespace
{

/// Prints one checked test case as it finishes, so a long run shows what it is doing rather than only its tally.
void printProgress(ProgressTracker& progress, const TestCaseId& id, const Verdict& verdict)
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

/// How many rounds the repetition asks for. Zero stands for no round limit, which only the time limit or a failure ends.
uint64_t roundsOf(const RepetitionPolicy& repetition)
{
    if (const auto* fixed = std::get_if<SubmitRounds>(&repetition))
    {
        return fixed->count;
    }
    return std::holds_alternative<SubmitUntilStopped>(repetition) ? 0 : 1;
}

/// Records how long one checked test case ran, keeping each query's best time across rounds.
/// Only a query that ran and passed is measured: a differential block has no input files, an EXPLAIN never runs,
/// and a negative test would measure the time to its failure.
void recordTiming(
    Benchmark& benchmark,
    const TestCaseId& id,
    const RewrittenTestCase& testCase,
    const Verdict& verdict,
    const std::span<const QueryTiming> timings)
{
    if (const auto* query = std::get_if<RewrittenQuery>(&testCase.action);
        query != nullptr and verdict.has_value() and not timings.empty() and not std::holds_alternative<ExpectedError>(query->expectation))
    {
        benchmark.record(fmt::format("{}", id), query->inputFiles, timings.front().execution);
    }
}

}

Executor::Executor(SystestConfiguration config) : config{std::move(config)}
{
}

std::expected<std::vector<RunnablePartition>, ReportEntry>
Executor::prepare(TestFileRewriter& rewriter, const DiscoveredTestFile& discoveredTestFile)
{
    try
    {
        return rewriter.rewrite(discoveredTestFile);
    }
    catch (const std::exception& exception)
    {
        const std::string_view message{exception.what()};
        return std::unexpected(ReportEntry{
            .id = TestCaseId{.originFile = discoveredTestFile.getName().getRawValue(), .queryIdInFile = std::nullopt, .overrides = {}},
            .outcome = Verdict{std::unexpected(Mismatch{
                fmt::format("could not prepare: {}", message.empty() ? fmt::format("{}", getCurrentErrorCode()) : std::string{message})})},
            .timings = {}});
    }
}

Executor::PreparedRun Executor::prepareAll(TestFileRewriter& rewriter)
{
    PreparedRun run;
    for (const auto& discoveredTestFile : discoverTestFiles(config))
    {
        fmt::print("Loading queries from test file: file://{}\n", discoveredTestFile.getLogFilePath());
        if (auto preparedTestFile = prepare(rewriter, discoveredTestFile))
        {
            std::ranges::move(*preparedTestFile, std::back_inserter(run.runnablePartitions));
        }
        else
        {
            run.failedTestFiles.push_back(std::move(preparedTestFile.error()));
        }
    }
    return run;
}

ExecutorResult Executor::summarize(const std::vector<ReportEntry>& checkedCases)
{
    /// Reporting success when no case ran would let a hidden group name or a filter that matches nothing go unnoticed.
    if (checkedCases.empty())
    {
        return RunFailed{
            .report = "no query ran: the test location, the groups and the disable config select nothing\n",
            .errorCode = ErrorCode::TestException};
    }

    std::string details;
    /// Skips are grouped per file partition and reason, because every test case of a partition skips for the same reason.
    std::map<std::pair<std::string, std::string>, size_t> skips;
    for (const auto& checked : checkedCases)
    {
        if (const auto* skip = std::get_if<Skipped>(&checked.outcome))
        {
            auto label = checked.id;
            label.queryIdInFile.reset();
            ++skips[{fmt::format("{}", label), skip->reason}];
        }
        else if (const auto& verdict = std::get<Verdict>(checked.outcome); not verdict.has_value())
        {
            details += fmt::format("  FAIL  {}: {}\n", checked.id, verdict.error().detail);
        }
    }
    for (const auto& [key, count] : skips)
    {
        details += fmt::format("  SKIP  {}: {} test cases: {}\n", key.first, count, key.second);
    }

    const auto skipped = static_cast<size_t>(
        std::ranges::count_if(checkedCases, [](const ReportEntry& checked) { return std::holds_alternative<Skipped>(checked.outcome); }));
    const auto passes
        = static_cast<size_t>(std::ranges::count_if(checkedCases, [](const ReportEntry& checked) { return hasPassed(checked.outcome); }));
    const auto failed = checkedCases.size() - passes - skipped;
    /// The skip tally appears only when something skipped, so the everyday report reads as before.
    const auto headline = skipped > 0 ? fmt::format("{} queries passed, {} failed, {} skipped\n", passes, failed, skipped)
                                      : fmt::format("{} queries passed, {} failed\n", passes, failed);
    if (failed == 0)
    {
        return RunSucceeded{.report = headline + details};
    }
    return RunFailed{.report = headline + details, .errorCode = ErrorCode::TestException};
}

ExecutorResult Executor::runOnce(TestRunner& runner, const RunPolicy& plan, PreparedRun prepared)
{
    ProgressTracker progress{std::ranges::fold_left(
        prepared.runnablePartitions
            | std::views::transform([](const auto& partition) -> size_t { return partition.test.testCases.size(); }),
        size_t{0},
        std::plus{})};

    /// What failed before any query ran joins the report next to the test cases that run below.
    auto setUp = runner.setUpAll(prepared.runnablePartitions);
    std::vector<ReportEntry> report = std::move(prepared.failedTestFiles);
    std::ranges::move(setUp.rejected, std::back_inserter(report));

    std::optional<Benchmark> benchmark;
    if (plan.measureReport.has_value())
    {
        benchmark.emplace();
    }

    const TestRunner::QueryObserver observe
        = [&](const TestCaseId& id, const RewrittenTestCase& testCase, const Verdict& verdict, const std::span<const QueryTiming> timings)
    {
        printProgress(progress, id, verdict);
        if (benchmark.has_value())
        {
            recordTiming(*benchmark, id, testCase, verdict, timings);
        }
    };

    std::ranges::move(runner.submitQueries(setUp.ready, plan.concurrency, observe), std::back_inserter(report));
    auto result = summarize(report);
    if (benchmark.has_value())
    {
        const auto written = benchmark->writeTo(*plan.measureReport);
        std::visit([&](auto& outcome) { outcome.report += written; }, result);
    }
    return result;
}

ExecutorResult Executor::runRounds(TestRunner& runner, const RunPolicy& plan, const PreparedRun& prepared)
{
    /// Every round has to cover the whole set, otherwise the rounds report metrics for a smaller set than what was given.
    if (not prepared.failedTestFiles.empty())
    {
        return summarize(prepared.failedTestFiles);
    }
    auto setUp = runner.setUpAll(prepared.runnablePartitions);
    if (not setUp.rejected.empty())
    {
        return summarize(setUp.rejected);
    }
    /// A selection that matches nothing would otherwise repeat an empty round forever.
    if (std::ranges::all_of(setUp.ready, [](const auto& runnable) { return runnable.get().testCases.empty(); }))
    {
        return summarize({});
    }

    /// Measuring is the only difference to a load run: each round then records how long every passing query took, and
    /// the run ends in the written report rather than a tally. The checks keep running underneath either way, because
    /// a fast wrong answer is not a measurement.
    const bool measuring = plan.measureReport.has_value();
    Benchmark benchmark;
    const TestRunner::QueryObserver observe
        = [&](const TestCaseId& id, const RewrittenTestCase& testCase, const Verdict& verdict, const std::span<const QueryTiming> timings)
    { recordTiming(benchmark, id, testCase, verdict, timings); };

    const auto rounds = roundsOf(plan.repetition);
    if (not measuring)
    {
        fmt::print("Repeating the queries of {} test files\n", setUp.ready.size());
    }

    const auto startedAt = std::chrono::steady_clock::now();
    for (uint64_t round = 1; rounds == 0 or round <= rounds; ++round)
    {
        const auto roundStartedAt = std::chrono::steady_clock::now();
        const auto checked = measuring ? runner.submitQueries(setUp.ready, plan.concurrency, observe)
                                       : runner.submitQueries(setUp.ready, plan.concurrency);
        const auto failed = std::ranges::count_if(checked, [](const ReportEntry& query) { return not hasPassed(query.outcome); });
        if (measuring)
        {
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

    if (measuring)
    {
        return RunSucceeded{.report = benchmark.writeTo(*plan.measureReport)};
    }
    return RunSucceeded{.report = "every round passed\n"};
}

ExecutorResult Executor::execute()
{
    setupLogging(config);
    const auto runPolicy = RunPolicy::create(config);
    const WorkingDirectoryGuard workingDirectoryGuard{config.workingDir.getValue()};

    /// One coordinator with the workers this invocation configured serves every one of its test files.
    TestRunner runner{config};
    /// The rewriter asks the runner where each partition goes, which registers the worker for that partition's settings.
    TestFileRewriter rewriter{config, [&runner](const ConfigurationOverride& settings) { return runner.placementFor(settings); }};
    auto prepared = prepareAll(rewriter);
    if (const auto* shuffle = std::get_if<RunInShuffledOrder>(&runPolicy.ordering))
    {
        /// The seed is printed, so a failure this finds can be repeated with the same seed.
        /// That repeats the order the files are submitted in, not the order their queries finish, which the pool decides.
        const uint64_t seed = shuffle->seed.has_value() ? *shuffle->seed : std::random_device{}();
        fmt::print("Running {} test files in random order, with seed {}\n", prepared.runnablePartitions.size(), seed);
        std::ranges::shuffle(prepared.runnablePartitions, std::mt19937_64{seed});
        /// The test cases of a file are shuffled too, so a test case that depends on the one written above it is found.
        for (auto& [_, test] : prepared.runnablePartitions)
        {
            std::ranges::shuffle(test.testCases, std::mt19937{std::random_device{}()});
        }
    }

    if (std::holds_alternative<SubmitOnce>(runPolicy.repetition))
    {
        return runOnce(runner, runPolicy, std::move(prepared));
    }
    return runRounds(runner, runPolicy, prepared);
}

}

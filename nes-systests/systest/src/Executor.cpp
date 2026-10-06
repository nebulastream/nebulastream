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
#include <exception>
#include <functional>
#include <iterator>
#include <map>
#include <optional>
#include <random>
#include <ranges>
#include <ratio>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include <fmt/base.h>
#include <fmt/format.h>

#include <Config/RunPolicy.hpp>
#include <Discovery/TestDiscovery.hpp>
#include <Model/RunnablePartition.hpp>
#include <Model/RunnableTestFile.hpp>
#include <Model/TestCaseId.hpp>
#include <Model/Verdict.hpp>
#include <Rewriter/TestFileRewriter.hpp>
#include <Runner/TestRunner.hpp>
#include <Benchmark.hpp>
#include <ErrorHandling.hpp>
#include <Logging.hpp>
#include <WorkingDirectoryGuard.hpp>

namespace NES
{
namespace
{

/// Sums the worker-measured time of the statements. A test case that ran nothing on the worker shows no time.
void printProgress(const size_t done, const size_t total, const ReportEntry& entry, const std::span<const StatementTiming> timings)
{
    const auto& verdict = std::get<Verdict>(entry.outcome);
    const auto execution = std::ranges::fold_left(
        timings, std::chrono::nanoseconds{}, [](const auto sum, const StatementTiming& timing) { return sum + timing.execution; });
    const auto took = execution > std::chrono::nanoseconds{}
        ? fmt::format(" in {:.1f} ms", std::chrono::duration<double, std::milli>{execution}.count())
        : std::string{};
    fmt::print("[{}/{}] {:.<100} {}{}\n", done, total, entry.id, verdict.has_value() ? "PASSED" : "FAILED", took);
    if (not verdict.has_value())
    {
        fmt::print("{}\n", verdict.error().detail);
    }
    static_cast<void>(std::fflush(stdout));
}

/// Shuffling exposes tests that depend on the order of other tests.
/// The seed is printed, so the order that exposed a failure can be repeated.
/// It repeats the order of submission, not the order in which concurrent queries finish.
void shuffle(std::vector<RunnablePartition>& partitions, const RunInShuffledOrder& ordering)
{
    const uint64_t seed = ordering.seed.has_value() ? *ordering.seed : uint64_t{std::random_device{}()};
    fmt::print("Running {} test files in random order, with seed {}\n", partitions.size(), seed);
    std::mt19937_64 engine{seed};
    std::ranges::shuffle(partitions, engine);
    for (auto& [_, file] : partitions)
    {
        std::ranges::shuffle(file.testCases, engine);
    }
}

/// Zero means no round limit, so only the time limit or a failed round ends the run.
uint64_t countRounds(const RepetitionPolicy& repetition)
{
    if (const auto* rounds = std::get_if<SubmitRounds>(&repetition))
    {
        return rounds->count;
    }
    return std::holds_alternative<SubmitUntilStopped>(repetition) ? 0 : 1;
}

struct RewrittenRun
{
    std::vector<RunnablePartition> partitions;
    std::vector<ReportEntry> rejected;
};

/// A file that cannot be read, parsed, or rewritten becomes one failed check, and the run goes on.
RewrittenRun rewriteAll(TestFileRewriter& rewriter, const SystestConfiguration& config)
{
    RewrittenRun run;
    for (const auto& discoveredTestFile : discoverTestFiles(config))
    {
        fmt::print("Loading queries from test file: file://{}\n", discoveredTestFile.getLogFilePath());
        try
        {
            std::ranges::move(rewriter.rewrite(discoveredTestFile), std::back_inserter(run.partitions));
        }
        catch (const std::exception& exception)
        {
            run.rejected.push_back(createFailedFileEntry(discoveredTestFile.getName().getRawValue(), {}, "could not prepare", exception));
        }
    }
    return run;
}

RunOutcome runOnce(const SystestConfiguration& config, const RunPolicy& policy, RewrittenRun rewritten)
{
    std::vector<ReportEntry> report = std::move(rewritten.rejected);
    TestRunner runner{config, std::move(rewritten.partitions)};
    std::ranges::copy(runner.getRejected(), std::back_inserter(report));
    const auto totalTestCases = runner.countTestCases();

    size_t checkedSoFar = 0;
    const TestRunner::Observer observe
        = [&](const ReportEntry& entry, const RewrittenTestCase&, const std::span<const StatementTiming> timings)
    { printProgress(++checkedSoFar, totalTestCases, entry, timings); };

    std::ranges::move(runner.submitAll(policy.concurrency, observe), std::back_inserter(report));
    return summarize(report);
}

/// Sets up once, then resubmits the bound plans every round until the rounds or the time run out, or a round fails.
/// A measuring run keeps each query's best time across rounds.
RunOutcome runRounds(const SystestConfiguration& config, const RunPolicy& policy, RewrittenRun rewritten)
{
    /// A rejected file would shrink every round's set, and an empty set would loop forever, so the run ends here.
    if (not rewritten.rejected.empty() or rewritten.partitions.empty())
    {
        return summarize(rewritten.rejected);
    }
    const auto fileCount = rewritten.partitions.size();
    TestRunner runner{config, std::move(rewritten.partitions)};
    if (not runner.getRejected().empty())
    {
        return summarize(runner.getRejected());
    }

    auto benchmark = policy.measureReport.has_value() ? std::optional{Benchmark{}} : std::nullopt;
    const TestRunner::Observer observe
        = [&](const ReportEntry& entry, const RewrittenTestCase& testCase, const std::span<const StatementTiming> timings)
    {
        if (benchmark.has_value())
        {
            benchmark->record(entry, testCase, timings);
        }
    };

    const auto rounds = countRounds(policy.repetition);
    fmt::print("Repeating the queries of {} test files\n", fileCount);
    const auto startedAt = std::chrono::steady_clock::now();
    std::vector<ReportEntry> checked;
    for (uint64_t round = 1; rounds == 0 or round <= rounds; ++round)
    {
        const auto roundStartedAt = std::chrono::steady_clock::now();
        checked = runner.submitAll(policy.concurrency, observe);
        const auto failed = std::ranges::count_if(checked, [](const ReportEntry& entry) { return not hasPassed(entry.outcome); });
        fmt::print(
            "round {}: {} passed, {} failed in {} ms\n",
            round,
            checked.size() - static_cast<size_t>(failed),
            failed,
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - roundStartedAt).count());
        static_cast<void>(std::fflush(stdout));

        /// The rounds after a failed one would measure or load the same wrong query.
        if (failed > 0)
        {
            return summarize(checked);
        }
        if (policy.runLimit.has_value() and std::chrono::steady_clock::now() - startedAt >= *policy.runLimit)
        {
            break;
        }
    }
    return summarize(
        checked, benchmark.has_value() ? Benchmark::writeReport(benchmark->buildRows(), *policy.measureReport) : std::string{});
}

}

RunOutcome summarize(const std::vector<ReportEntry>& entries, const std::string_view appendix)
{
    /// An empty run fails, so a filter that selects nothing does not pass unnoticed.
    if (entries.empty())
    {
        return RunFailed{
            .report = fmt::format("no query ran: the test location, the groups and the disable config select nothing\n{}", appendix),
            .errorCode = ErrorCode::TestException};
    }

    std::string details;
    /// One SKIP line per partition and reason: a partition's test cases skip together.
    std::map<std::pair<std::string, std::string>, size_t> skips;
    for (const auto& entry : entries)
    {
        if (const auto* skip = std::get_if<Skipped>(&entry.outcome))
        {
            auto label = entry.id;
            label.queryIdInFile.reset();
            ++skips[{fmt::format("{}", label), skip->reason}];
        }
        else if (const auto& verdict = std::get<Verdict>(entry.outcome); not verdict.has_value())
        {
            details += fmt::format("  FAIL  {}: {}\n", entry.id, verdict.error().detail);
        }
    }
    for (const auto& [key, count] : skips)
    {
        const auto& [label, reason] = key;
        details += fmt::format("  SKIP  {}: {} test cases: {}\n", label, count, reason);
    }

    const auto skipped = std::ranges::fold_left(skips | std::views::values, size_t{0}, std::plus{});
    const auto passes
        = static_cast<size_t>(std::ranges::count_if(entries, [](const ReportEntry& entry) { return hasPassed(entry.outcome); }));
    const auto failed = entries.size() - passes - skipped;
    const auto headline = skipped > 0 ? fmt::format("{} queries passed, {} failed, {} skipped\n", passes, failed, skipped)
                                      : fmt::format("{} queries passed, {} failed\n", passes, failed);
    if (failed == 0)
    {
        return RunSucceeded{.report = fmt::format("{}{}{}", headline, details, appendix)};
    }
    return RunFailed{.report = fmt::format("{}{}{}", headline, details, appendix), .errorCode = ErrorCode::TestException};
}

Executor::Executor(SystestConfiguration config) : config{std::move(config)}
{
}

RunOutcome Executor::execute()
{
    setupLogging(config);
    const auto runPolicy = RunPolicy::create(config);
    const WorkingDirectoryGuard workingDirectoryGuard{config.workingDir.getValue()};

    /// One rewriter per run, because it records the names that each file claimed.
    TestFileRewriter rewriter{config};
    auto rewritten = rewriteAll(rewriter, config);
    if (const auto* ordering = std::get_if<RunInShuffledOrder>(&runPolicy.ordering))
    {
        shuffle(rewritten.partitions, *ordering);
    }

    return std::holds_alternative<SubmitOnce>(runPolicy.repetition) ? runOnce(config, runPolicy, std::move(rewritten))
                                                                    : runRounds(config, runPolicy, std::move(rewritten));
}

}

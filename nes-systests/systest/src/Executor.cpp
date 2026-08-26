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
#include <Rewriter/TestFilePreparer.hpp>
#include <Runner/TestRunner.hpp>
#include <Benchmark.hpp>
#include <ErrorHandling.hpp>
#include <Logging.hpp>
#include <WorkingDirectoryGuard.hpp>

namespace NES
{
namespace
{

/// Sums the worker-measured time of the statements.
/// Only valid to call with an outcome that is a verdict (not skipped cases, those should not have a timing, etc.)
void printProgress(const size_t done, const size_t total, const ReportEntry& entry, const std::span<const StatementTiming> timings)
{
    PRECONDITION(std::holds_alternative<Verdict>(entry.outcome), "only valid to call with an outcome that is a verdict");
    const auto& verdict = std::get<Verdict>(entry.outcome);
    const auto execution = std::ranges::fold_left(
        timings, std::chrono::nanoseconds{}, [](const auto sum, const StatementTiming& timing) { return sum + timing.execution; });
    const auto tookMessage = execution > std::chrono::nanoseconds{}
        ? fmt::format(" in {:.1f} ms", std::chrono::duration<double, std::milli>{execution}.count())
        : std::string{};
    fmt::print("[{}/{}] {:.<100} {}{}\n", done, total, entry.id, verdict.has_value() ? "PASSED" : "FAILED", tookMessage);
    if (not verdict.has_value())
    {
        fmt::print("{}\n", verdict.error().detail);
    }
    static_cast<void>(std::fflush(stdout));
}

/// Shuffling exposes tests that depend on the order of other tests.
/// This shuffles both the test file partitions and the cases within each partition.
void shuffle(std::vector<RunnablePartition>& partitions)
{
    std::ranges::shuffle(partitions, std::mt19937{std::random_device{}()});
    for (auto& [_, file] : partitions)
    {
        std::ranges::shuffle(file.testCases, std::mt19937{std::random_device{}()});
    }
}

/// Type that holds all test file partitions that should be tested in the current invocation.
/// Partitions that fail during this stage will be put into the `rejected` field.
struct PreparedRun
{
    std::vector<RunnablePartition> partitions;
    std::vector<ReportEntry> rejected;
};

/// This prepares the tests into a PreparedRun, no test case or setup statement is submitted yet.
/// We discover the test files from the given config, and prepare each of them, collecting prepared test file partitions.
/// If a file cannot be read, parsed, or rewritten, it becomes a failed check (which does not end the run).
PreparedRun prepareAll(const SystestConfiguration& config)
{
    TestFilePreparer preparer{config};
    PreparedRun run;
    for (const auto& discoveredTestFile : discoverTestFiles(config))
    {
        fmt::print("Loading queries from test file: file://{}\n", discoveredTestFile.getLogFilePath());
        try
        {
            std::ranges::move(preparer.prepare(discoveredTestFile), std::back_inserter(run.partitions));
        }
        catch (const std::exception& exception)
        {
            run.rejected.push_back(createFailedFileEntry(discoveredTestFile.getName().getRawValue(), {}, "could not prepare", exception));
        }
    }
    return run;
}

RunOutcome runOnce(const SystestConfiguration& config, const RunPolicy& policy, PreparedRun prepared)
{
    std::vector<ReportEntry> report = std::move(prepared.rejected);
    TestRunner runner{config, std::move(prepared.partitions)};
    std::ranges::copy(runner.getRejected(), std::back_inserter(report));
    const auto totalTestCases = runner.countTestCases();

    size_t checkedSoFar = 0;
    auto benchmark = policy.measureReport.has_value() ? std::optional{Benchmark{}} : std::nullopt;
    const TestRunner::Observer observe
        = [&](const ReportEntry& entry, const RewrittenTestCase& testCase, const std::span<const StatementTiming> timings)
    {
        printProgress(++checkedSoFar, totalTestCases, entry, timings);
        if (benchmark.has_value())
        {
            benchmark->record(entry, testCase, timings);
        }
    };

    std::ranges::move(runner.submitAll(policy.concurrency, observe), std::back_inserter(report));
    return summarize(report, benchmark.has_value() ? Benchmark::writeReport(benchmark->buildRows(), *policy.measureReport) : std::string{});
}

/// Sets up once, then resubmits the bound plans every round until one fails.
RunOutcome runRounds(const SystestConfiguration& config, const RunPolicy& policy, PreparedRun rewritten)
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

    fmt::print("Repeating the queries of {} test files\n", fileCount);
    for (size_t round = 1;; ++round)
    {
        const auto roundStartedAt = std::chrono::steady_clock::now();
        const auto checked = runner.submitAll(policy.concurrency);
        const auto failed = std::ranges::count_if(checked, [](const ReportEntry& entry) { return not hasPassed(entry.outcome); });
        fmt::print(
            "round {}: {} passed, {} failed in {} ms\n",
            round,
            checked.size() - static_cast<size_t>(failed),
            failed,
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - roundStartedAt).count());
        static_cast<void>(std::fflush(stdout));

        if (failed > 0)
        {
            return summarize(checked);
        }
    }
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

RunOutcome Executor::execute() const
{
    setupLogging(config);
    const auto runPolicy = RunPolicy::create(config);
    /// Protect the working dir during the invocation from concurrent invications on the same dir or a subdir
    const auto guard = WorkingDirectoryGuard{config.workingDir.getValue()};

    auto prepared = prepareAll(config);
    if (std::holds_alternative<RunInShuffledOrder>(runPolicy.ordering))
    {
        shuffle(prepared.partitions);
    }

    return std::holds_alternative<SubmitOnce>(runPolicy.repetition) ? runOnce(config, runPolicy, std::move(prepared))
                                                                    : runRounds(config, runPolicy, std::move(prepared));
}

}

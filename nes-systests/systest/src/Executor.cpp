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
#include <nes-coordinator-bridge/coordinator.h>
#include <BridgeConfig.hpp>

#include <Config/RunPolicy.hpp>
#include <Discovery/TestDiscovery.hpp>
#include <Model/ConfigurationOverride.hpp>
#include <Model/RunnablePartition.hpp>
#include <Model/RunnableTestFile.hpp>
#include <Model/TestCaseId.hpp>
#include <Model/Verdict.hpp>
#include <Rewriter/TestFilePreparer.hpp>
#include <Runner/Cluster.hpp>
#include <Runner/TestRunner.hpp>
#include <Benchmark.hpp>
#include <ErrorHandling.hpp>
#include <Logging.hpp>
#include <WorkingDirectoryGuard.hpp>

namespace NES
{

/// Shuffling exposes tests that depend on the order of other tests.
/// This shuffles both the test file partitions and the cases within each partition.
/// It prints the seed, so a rerun with that seed repeats the order of submission.
/// The seed does not fix the order in which concurrent queries finish.
static void shuffle(std::vector<RunnablePartition>& partitions, const RunInShuffledOrder& ordering)
{
    fmt::print("Running {} test files in random order, with seed {}\n", partitions.size(), ordering.seed);
    std::mt19937_64 engine{ordering.seed};
    std::ranges::shuffle(partitions, engine);
    for (auto& [_, file] : partitions)
    {
        std::ranges::shuffle(file.testCases, engine);
    }
}

/// Zero means no round limit, so only the time limit or a failed round ends the run.
static uint64_t countRounds(const RepetitionPolicy& repetition)
{
    if (const auto* rounds = std::get_if<SubmitRounds>(&repetition))
    {
        return rounds->count;
    }
    return std::holds_alternative<SubmitUntilStopped>(repetition) ? 0 : 1;
}

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

size_t countTestCases(const std::vector<RunnablePartition>& partitions)
{
    return std::ranges::fold_left(
        partitions, size_t{0}, [](const size_t sum, const RunnablePartition& partition) { return sum + partition.file.testCases.size(); });
}

/// The partitions with equal configuration overrides, which run on the same cluster.
struct SettingsGroup
{
    ConfigurationOverride overrides;
    std::vector<RunnablePartition> partitions;
    /// The index of each partition among all partitions of the run.
    std::vector<size_t> positions;
};

/// Returns the groups in the order of their first partitions.
std::vector<SettingsGroup> groupBySettings(std::vector<RunnablePartition> partitions)
{
    std::vector<SettingsGroup> groups;
    for (size_t position = 0; position < partitions.size(); ++position)
    {
        auto& partition = partitions.at(position);
        const auto found = std::ranges::find(groups, partition.overrides, &SettingsGroup::overrides);
        auto& group = found == groups.end()
            ? groups.emplace_back(SettingsGroup{.overrides = partition.overrides, .partitions = {}, .positions = {}})
            : *found;
        group.positions.push_back(position);
        group.partitions.push_back(std::move(partition));
    }
    return groups;
}

/// A report entry with the index of its partition among all partitions of the run.
struct PositionedEntry
{
    size_t position;
    ReportEntry entry;
};

/// The groups reorder the execution, and the report still follows the order of the partitions.
std::vector<ReportEntry> orderByPosition(std::vector<PositionedEntry> entries)
{
    std::ranges::stable_sort(entries, {}, &PositionedEntry::position);
    return entries | std::views::transform([](PositionedEntry& positioned) { return std::move(positioned.entry); })
        | std::ranges::to<std::vector>();
}

/// The result of running every settings group once.
struct GroupsOutcome
{
    /// The files whose setup failed: one entry for the failure and one skip per test case.
    std::vector<ReportEntry> rejected;
    std::vector<ReportEntry> checked;
};

/// Runs the settings groups one after another, each on its own cluster.
/// A worker reserves its whole memory budget when it starts, so this function destroys the cluster of one group before it starts the next.
/// `onRejected` receives the number of test cases that a group does not run, because the setup of their files failed.
/// The function skips an empty `onRejected`.
GroupsOutcome runGroups(
    const SystestConfiguration& config,
    const Cluster::Config& baseCluster,
    const size_t concurrency,
    const std::vector<SettingsGroup>& groups,
    const TestRunner::Observer& observe,
    const std::function<void(size_t)>& onRejected)
{
    std::vector<PositionedEntry> rejected;
    std::vector<PositionedEntry> checked;
    for (const auto& group : groups)
    {
        /// A file has at most one partition per set of overrides, so within a group the file identifies the partition.
        const auto positionOf = [&group](const ReportEntry& entry)
        {
            const auto partition = std::ranges::find(
                group.partitions,
                entry.id.originFile,
                [](const RunnablePartition& candidate) -> const std::string& { return candidate.file.name; });
            INVARIANT(partition != group.partitions.end(), "a group reports only the files that it ran");
            return group.positions.at(static_cast<size_t>(std::ranges::distance(group.partitions.begin(), partition)));
        };

        auto clusterConfig = baseCluster;
        clusterConfig.overrides = group.overrides;
        const Cluster cluster{clusterConfig};
        TestRunner runner{config, cluster, group.partitions};
        for (const auto& entry : runner.getRejected())
        {
            rejected.push_back(PositionedEntry{.position = positionOf(entry), .entry = entry});
        }
        if (onRejected)
        {
            onRejected(countTestCases(group.partitions) - runner.countTestCases());
        }
        for (auto& entry : runner.submitAll(concurrency, observe))
        {
            const auto position = positionOf(entry);
            checked.push_back(PositionedEntry{.position = position, .entry = std::move(entry)});
        }
    }
    return GroupsOutcome{.rejected = orderByPosition(std::move(rejected)), .checked = orderByPosition(std::move(checked))};
}

RunOutcome runOnce(const SystestConfiguration& config, const Cluster::Config& baseCluster, const RunPolicy& policy, PreparedRun prepared)
{
    std::vector<ReportEntry> report = std::move(prepared.rejected);
    const auto totalTestCases = countTestCases(prepared.partitions);
    const auto groups = groupBySettings(std::move(prepared.partitions));

    size_t checkedSoFar = 0;
    const TestRunner::Observer observe
        = [&](const ReportEntry& entry, const RewrittenTestCase&, const std::span<const StatementTiming> timings)
    { printProgress(++checkedSoFar, totalTestCases, entry, timings); };
    /// The test cases of a rejected file are never checked, so the progress counts them here.
    const auto countRejected = [&](const size_t rejectedTestCases) { checkedSoFar += rejectedTestCases; };

    auto [rejected, checked] = runGroups(config, baseCluster, policy.concurrency, groups, observe, countRejected);
    std::ranges::move(rejected, std::back_inserter(report));
    std::ranges::move(checked, std::back_inserter(report));
    return summarize(report);
}

/// Runs every settings group once per round, until the rounds or the time run out, or a round fails.
/// Each round starts new clusters and repeats the setup of the files.
/// A measuring run keeps each query's best time across rounds.
RunOutcome runRounds(const SystestConfiguration& config, const Cluster::Config& baseCluster, const RunPolicy& policy, PreparedRun rewritten)
{
    /// A rejected file would shrink every round's set, and an empty set would loop forever, so the run ends here.
    if (not rewritten.rejected.empty() or rewritten.partitions.empty())
    {
        return summarize(rewritten.rejected);
    }
    const auto fileCount = rewritten.partitions.size();
    const auto groups = groupBySettings(std::move(rewritten.partitions));

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
        auto [rejected, checkedInRound] = runGroups(config, baseCluster, policy.concurrency, groups, observe, {});
        /// A file whose setup failed would shrink every round's set as well, so the run ends with its report.
        if (not rejected.empty())
        {
            return summarize(rejected);
        }
        checked = std::move(checkedInRound);
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

RunOutcome Executor::execute() const
{
    setupLogging(config);
    const auto runPolicy = RunPolicy::create(config);
    /// Protect the working dir during the invocation from concurrent invications on the same dir or a subdir
    const auto guard = WorkingDirectoryGuard{config.workingDir};

    const auto mode = config.remoteWorker ? Bridge::WorkerMode::Remote : Bridge::WorkerMode::Embedded;
    const auto [optimizerLiterals, workerLiterals] = Bridge::splitOptimizerConfig(config.workerOptimizerConfigLiterals);
    const Cluster::Config baseCluster{
        .mode = mode,
        .topology = config.clusterConfig,
        .optimizerConfig = optimizerLiterals,
        .workerConfig = workerLiterals,
        .overrides = {}};

    auto prepared = prepareAll(config);
    if (const auto* ordering = std::get_if<RunInShuffledOrder>(&runPolicy.ordering))
    {
        shuffle(prepared.partitions, *ordering);
    }

    return std::holds_alternative<SubmitOnce>(runPolicy.repetition) ? runOnce(config, baseCluster, runPolicy, std::move(prepared))
                                                                    : runRounds(config, baseCluster, runPolicy, std::move(prepared));
}

}

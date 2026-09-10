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
#include <expected>
#include <functional>
#include <iterator>
#include <optional>
#include <random>
#include <ranges>
#include <span>
#include <string>
#include <tuple>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include <cpptrace/from_current.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>

#include <Config/RunPlan.hpp>
#include <Discovery/TestDiscovery.hpp>
#include <Discovery/TestFileReader.hpp>
#include <Model/RunnableTest.hpp>
#include <Model/SystestQueryId.hpp>
#include <Model/TestCaseId.hpp>
#include <Model/Verdict.hpp>
#include <Parser/SystestParser.hpp>
#include <Parser/TestFileParser.hpp>
#include <Rewriter/NameQualifier.hpp>
#include <Rewriter/SqlRewriter.hpp>
#include <Runner/TestRunner.hpp>
#include <Util/Overloaded.hpp>
#include <Benchmark.hpp>
#include <ErrorHandling.hpp>
#include <Logging.hpp>
#include <Progress.hpp>
#include <TestFilePartition.hpp>
#include <WorkingDirectoryGuard.hpp>

namespace NES
{
namespace
{

/// Runs the test files in a random order, so a test that only passes because another one ran before it is found.
/// The queries within one file keep their order, because the pool already interleaves them and the order a file states
/// is relative to the query above rather than to a place in the run.
/// The seed is printed, so a failure this finds can be repeated with `--shuffle-seed`.
/// That repeats the order the files are submitted in, not the order their queries finish, which the pool decides.
void shuffleTestFiles(std::vector<RunnableTest>& prepared, const Shuffled& order)
{
    const uint64_t seed = order.seed.has_value() ? *order.seed : std::random_device{}();
    fmt::print("Running {} test files in random order, with seed {}\n", prepared.size(), seed);
    std::mt19937_64 generator{seed};
    std::ranges::shuffle(prepared, generator);
}

/// Drops the cases a run did not select, keeping every one the selection depends on.
/// A case that has to follow the one above it needs that one to still be there, so the chain stays contiguous and the
/// dependency still points at it.
/// A differential block is one case covering both its query numbers, so selecting either number keeps the block.
void keepSelectedQueries(RunnableTest& runnable, const std::unordered_set<SystestQueryId>& selected)
{
    if (selected.empty())
    {
        return;
    }

    std::vector<bool> keep(runnable.cases.size(), false);
    for (size_t index = 0; index < runnable.cases.size(); ++index)
    {
        keep.at(index) = std::visit(
            Overloaded{
                [&](const RunnableQuery& query) { return selected.contains(query.id); },
                [&](const RunnableDifferential& differential)
                { return selected.contains(differential.firstId) or selected.contains(differential.secondId); }},
            runnable.cases.at(index).action);
    }
    for (size_t index = runnable.cases.size(); index > 1; --index)
    {
        if (keep.at(index - 1) and runnable.cases.at(index - 1).runsAfterPrevious)
        {
            keep.at(index - 2) = true;
        }
    }

    size_t index = 0;
    std::erase_if(runnable.cases, [&](const RunnableCase&) { return not keep.at(index++); });
}

}

Executor::Executor(Config config) : config(std::move(config))
{
}

std::expected<std::vector<RunnableTest>, CheckedQuery> Executor::prepare(const DiscoveredTestFile& discovered, TestRunner& runner) const
{
    /// Every failure below reports this name, so this reads it before anything can throw.
    const auto name = discovered.name();
    try
    {
        SystestParser parser;
        parser.loadString(readTestFile(discovered.file));
        const auto testFileKey = getTestFileKey(discovered.file, config.testsDiscoverDir.getValue());
        const auto testFile = parseTestFile(parser, discovered.file);

        const auto parts = partitionBySettings(testFile);
        std::vector<RunnableTest> prepared;
        prepared.reserve(parts.size());
        for (size_t index = 0; index < parts.size(); ++index)
        {
            const auto& [settings, statements] = parts.at(index);
            const auto placement = runner.placementFor(settings);
            if (not placement.has_value())
            {
                fmt::print("Skipping {} because it asks for worker settings the run cannot give it\n", name);
                continue;
            }
            SqlRewriter rewriter{RewriteTarget{
                .testFileKey = getTestFilePartKey(testFileKey, index, parts.size()),
                .displayName = name,
                .workingDir = config.workingDir.getValue(),
                .testDataDir = config.testDataDir.getValue(),
                .sourceHost = placement->sources,
                .sinkHost = placement->sinks}};
            auto runnable = rewriter.rewrite(statements);
            runnable.variant = static_cast<uint32_t>(index);
            keepSelectedQueries(runnable, discovered.onlyEnableQueriesWithTestQueryNumber);
            prepared.push_back(std::move(runnable));
        }
        return prepared;
    }
    catch (const Exception& exception)
    {
        const std::string message{exception.what()};
        return std::unexpected(CheckedQuery{
            .id = TestCaseId{.file = name, .variant = 0, .query = INVALID<SystestQueryId>},
            .outcome = Mismatch{fmt::format("could not prepare: {}", message.empty() ? fmt::format("{}", exception.code()) : message)},
            .timings = {}});
    }
    catch (const std::exception& exception)
    {
        return std::unexpected(CheckedQuery{
            .id = TestCaseId{.file = name, .variant = 0, .query = INVALID<SystestQueryId>},
            .outcome = Mismatch{fmt::format("could not prepare: {}", exception.what())},
            .timings = {}});
    }
}

Executor::PreparedRun Executor::prepareAll(TestRunner& runner) const
{
    PreparedRun run;
    for (const auto& discovered : discoverTestFiles(config))
    {
        if (auto ready = prepare(discovered, runner))
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
    /// An invocation that ran no query has not passed, it has not run.
    /// Reporting success would let a group name nobody kept up to date, or a filter that matches nothing, go unnoticed.
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
                [](const Passed&) {},
                [&](const Mismatch& mismatch) { failures.push_back(fmt::format("  FAIL  {}: {}\n", query.id, mismatch.detail)); },
                [&](const Skipped& skip)
                {
                    /// The label drops the query number, so every case of one file part folds into one line.
                    auto label = fmt::format(
                        "{}", TestCaseId{.file = query.id.file, .variant = query.id.variant, .query = INVALID<SystestQueryId>});
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

ExecutorResult
Executor::runOnce(TestRunner& runner, const RunPlan& plan, const std::vector<RunnableTest>& prepared, std::vector<CheckedQuery> unprepared)
{
    Progress progress{std::ranges::fold_left(
        prepared | std::views::transform([](const RunnableTest& file) { return file.cases.size(); }), size_t{0}, std::plus{})};
    progress.beginRun(prepared.size());

    /// What failed before any query ran joins the report next to the cases that run below.
    auto setUp = runner.setUpAll(prepared);
    std::vector<CheckedQuery> report = std::move(unprepared);
    std::ranges::move(setUp.rejected, std::back_inserter(report));

    const TestRunner::QueryObserver observe
        = [&](const TestCaseId& id, const RunnableCase& testCase, const Verdict& verdict, const std::span<const QueryTiming> timings)
    {
        const auto waited = std::ranges::fold_left(
            timings | std::views::transform(&QueryTiming::submission), std::chrono::steady_clock::duration{}, std::plus{});
        progress.report(id, testCase, verdict, waited);
    };
    std::ranges::move(runner.submitQueries(setUp.ready, plan.concurrency, observe), std::back_inserter(report));
    return summarize(report);
}

ExecutorResult Executor::runRounds(
    TestRunner& runner, const RunPlan& plan, const std::vector<RunnableTest>& prepared, std::vector<CheckedQuery> unprepared)
{
    /// A repeating run measures what it runs or puts it under load, and a file that could not be prepared or set up is
    /// a failure of the invocation rather than something to repeat.
    if (not unprepared.empty())
    {
        return summarize(unprepared);
    }
    auto setUp = runner.setUpAll(prepared);
    if (not setUp.rejected.empty())
    {
        return summarize(setUp.rejected);
    }

    /// Engaged when the run measures, which is the only difference to a load run: each round then records how long
    /// every passing query took, and the run ends in the written report rather than a tally.
    std::optional<Benchmark> benchmark;
    if (plan.measureReport.has_value())
    {
        benchmark.emplace();
    }

    /// Records what one round measured, reading the checks and the cases side by side: the runner reports in file
    /// order, so the check at each position belongs to the case at that position.
    /// A case that failed measured nothing, and the failure ends the run after its round anyway.
    /// A differential block yields one row per half, keeping the report keys the halves had when they were two queries.
    const auto recordRound = [&](const std::vector<CheckedQuery>& checked)
    {
        size_t position = 0;
        for (const RunnableTest& file : setUp.ready)
        {
            for (const auto& [action, _] : file.cases)
            {
                const auto& result = checked.at(position++);
                if (not std::holds_alternative<Passed>(result.outcome))
                {
                    continue;
                }
                std::visit(
                    Overloaded{
                        [&](const RunnableQuery& query)
                        {
                            benchmark->record(
                                fmt::format("{}:{}", file.name, query.id.getRawValue()),
                                query.inputFiles,
                                result.timings.front().execution);
                        },
                        [&](const RunnableDifferential& differential)
                        {
                            benchmark->record(
                                fmt::format("{}:{}", file.name, differential.firstId.getRawValue()),
                                differential.inputFiles,
                                result.timings.front().execution);
                            benchmark->record(
                                fmt::format("{}:{}", file.name, differential.secondId.getRawValue()),
                                differential.inputFiles,
                                result.timings.back().execution);
                        }},
                    action);
            }
        }
    };

    /// Zero stands for no round limit, which only the time limit or a failure ends.
    uint64_t rounds = 1;
    if (const auto* fixed = std::get_if<FixedRounds>(&plan.repetition))
    {
        rounds = fixed->count;
    }
    else if (std::holds_alternative<UntilLimit>(plan.repetition))
    {
        rounds = 0;
    }

    if (not benchmark.has_value())
    {
        fmt::print("Repeating the queries of {} test files\n", setUp.ready.size());
    }

    const auto startedAt = std::chrono::steady_clock::now();
    for (uint64_t round = 1; rounds == 0 or round <= rounds; ++round)
    {
        const auto roundStartedAt = std::chrono::steady_clock::now();
        const auto checked = runner.submitQueries(setUp.ready, plan.concurrency);
        const auto failed
            = std::ranges::count_if(checked, [](const CheckedQuery& query) { return std::holds_alternative<Mismatch>(query.outcome); });
        if (benchmark.has_value())
        {
            recordRound(checked);
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
        std::fflush(stdout);

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

    if (benchmark.has_value())
    {
        return RunSucceeded{.report = benchmark->writeTo(*plan.measureReport)};
    }
    return RunSucceeded{.report = "every round passed\n"};
}

ExecutorResult Executor::execute() const
{
    setupLogging(config);

    CPPTRACE_TRY
    {
        /// Held for the whole run, so a second run reports the conflict instead of clearing this run's files.
        const WorkingDirectoryGuard guard{config.workingDir.getValue()};

        /// One coordinator with the workers this invocation configured serves every one of its test files.
        TestRunner runner{RunSettings{
            .optimizer = config.optimizerOverrides,
            .cluster
            = {.mode = config.remoteWorker.getValue() ? WorkerMode::Remote : WorkerMode::Embedded,
               .topology = config.clusterConfig,
               .workerSettings = config.workerOverrides},
            .queryTimeout = std::chrono::seconds{config.queryTimeoutSeconds.getValue()}}};

        /// A test file that could not be prepared already has a verdict, and joins the verdicts the queries produce.
        auto [prepared, checked] = prepareAll(runner);
        const auto plan = RunPlan::create(config);
        if (const auto* shuffled = std::get_if<Shuffled>(&plan.ordering))
        {
            shuffleTestFiles(prepared, *shuffled);
        }
        if (std::holds_alternative<Once>(plan.repetition))
        {
            return runOnce(runner, plan, prepared, std::move(checked));
        }
        return runRounds(runner, plan, prepared, std::move(checked));
    }
    CPPTRACE_CATCH(const Exception& e)
    {
        tryLogCurrentException();
        const auto currentErrorCode = getCurrentErrorCode();
        return RunFailed{.report = fmt::format("Failed with exception: {}, {}", currentErrorCode, e.what()), .errorCode = currentErrorCode};
    }
    return RunFailed{.report = "Fatal error, should never reach this point.", .errorCode = ErrorCode::UnknownException};
}
}

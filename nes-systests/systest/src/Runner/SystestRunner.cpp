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

#include <Runner/SystestRunner.hpp>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <expected> /// NOLINT(misc-include-cleaner)
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <memory>
#include <optional>
#include <ostream>
#include <queue>
#include <ranges>
#include <regex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>
#include <Config/Config.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Model/Expectation.hpp>
#include <Model/TestCaseId.hpp>
#include <Model/Verdict.hpp>
#include <QueryManager/EmbeddedWorkerQuerySubmissionBackend.hpp>
#include <QueryManager/GRPCQuerySubmissionBackend.hpp>
#include <QueryManager/QueryManager.hpp>
#include <ResultChecker/Check.hpp>
#include <ResultChecker/DifferentialChecker.hpp>
#include <ResultChecker/ExplainChecker.hpp>
#include <ResultChecker/QueryResultChecker.hpp>
#include <Rewriter/NameQualifier.hpp>
#include <Runner/QuerySubmitter.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Variant.hpp>
#include <fmt/base.h>
#include <fmt/color.h>
#include <fmt/format.h>
#include <DistributedQuery.hpp>
#include <ErrorHandling.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <SystestState.hpp>
#include <WorkerCatalog.hpp>

namespace NES::Systest
{
namespace
{
void reportResult(
    std::shared_ptr<RunningQuery>& runningQuery,
    SystestProgressTracker& progressTracker,
    std::vector<std::shared_ptr<RunningQuery>>& failed,
    Verdict verdict,
    const QueryPerformanceMessageBuilder& performanceMessageBuilder)
{
    const bool mismatched = not verdict.has_value();
    runningQuery->verdict = std::move(verdict);

    std::string performanceMessage;
    /// Printing the query performance for any query that has not stoppped, e.g., failed, makes no sense
    if (performanceMessageBuilder
        and runningQuery->queryStatus.has_value()
        /// NOLINTNEXTLINE(bugprone-unchecked-optional-access) guarded by has_value() above
        and runningQuery->queryStatus->getGlobalQueryStatus() == DistributedQueryStatus::Stopped)
    {
        performanceMessage = performanceMessageBuilder(*runningQuery);
    }

    progressTracker.incrementQueryCounter();
    printQueryResultToStdOut(*runningQuery, progressTracker, performanceMessage);
    if (mismatched)
    {
        failed.push_back(runningQuery);
    }
}

bool passes(const std::shared_ptr<RunningQuery>& runningQuery)
{
    return runningQuery->verdict.has_value() and runningQuery->verdict->has_value();
}

/// Checks a query that reached a successful terminal state against what the test expects of its result.
Verdict checkSucceededQuery(const SystestQuery& query)
{
    if (std::holds_alternative<ExpectedError>(query.expectation))
    {
        return std::unexpected(
            Mismatch{fmt::format("expected error {} but query succeeded", std::get<ExpectedError>(query.expectation).code)});
    }

    if (query.differentialQueryPlan.has_value())
    {
        INVARIANT(
            query.resultFile.has_value() and query.differentialResultFile.has_value(),
            "a differential pair has to carry both result files");
        return runCheck(DifferentialCheck{.firstResultFile = *query.resultFile, .secondResultFile = *query.differentialResultFile});
    }

    /// A query without a result file writes none, such as one into a discarding sink, so there is nothing to check.
    /// Expected rows on such a query would never be compared, so they fail it instead of passing it silently.
    if (not query.resultFile.has_value())
    {
        if (const auto* rows = std::get_if<ExpectedRows>(&query.expectation); rows != nullptr and not rows->rows.empty())
        {
            return std::unexpected(Mismatch{"the test expects rows, but the query writes no result file to compare them against"});
        }
        NES_INFO("Skipping result check for {}:{} because it writes no result file.", query.testName, query.queryIdInFile);
        return Success{};
    }

    return runCheck(QueryResultCheck{
        .resultFile = *query.resultFile,
        .expectedSchema = query.planInfoOrException.value().sinkOutputSchema,
        .expectedTuples = NES::get<ExpectedRows>(query.expectation).rows});
}

/// Checks the plan an EXPLAIN printed, which the binder computed, because an EXPLAIN never reaches the worker.
/// The printed plan carries the qualified names, so the qualifying prefix comes off before the comparison and the
/// plan reads as the test wrote it.
Verdict checkExplainedQuery(const SystestQuery& query)
{
    if (std::holds_alternative<ExpectedError>(query.expectation))
    {
        return std::unexpected(
            Mismatch{fmt::format("expected error {} but EXPLAIN succeeded", std::get<ExpectedError>(query.expectation).code)});
    }

    INVARIANT(query.actualExplainOutput.has_value(), "checking an EXPLAIN requires a computed explain output");
    const auto expectedLines = NES::get<ExpectedPlan>(query.expectation).lines;
    const auto actual = unqualified(query.actualExplainOutput.value(), query.qualifyingPrefix);
    if (hasExplainRegexTags(expectedLines))
    {
        return runCheck(ExplainRegexCheck{.expected = expectedLines, .actual = actual});
    }
    return runCheck(ExplainLinesCheck{.expected = expectedLines, .actual = actual});
}

/// Checks a query that failed against the error the test expects.
/// Errors beyond the expected one are tolerated, because a failure on one pipeline can raise further errors on the pipelines
/// connected to it.
/// If the test also states an expected message, that message must appear verbatim in the matching exception,
/// so that tests can pin down the wording an error reports and not just its code.
Verdict checkFailedQuery(const std::optional<DistributedException>& failure, const Expectation& expectation)
{
    if (not failure.has_value())
    {
        return std::unexpected(Mismatch{"Query Failed without reporting an error"});
    }
    const DistributedException& actual = failure.value();

    const auto* expectedError = std::get_if<ExpectedError>(&expectation);
    if (expectedError == nullptr)
    {
        return std::unexpected(Mismatch{fmt::format("Query Failed with unexpected error: {}", actual)});
    }

    auto allExceptionByAddress = std::views::join(std::views::transform(
        actual.details(),
        [](auto& exceptionsByAddress)
        {
            return std::views::transform(
                exceptionsByAddress.second,
                [address = exceptionsByAddress.first](auto& exception) { return std::pair{address, std::cref(exception)}; });
        }));

    const auto expectedErrorOccurred = std::ranges::any_of(
        allExceptionByAddress | std::views::values,
        [&](const auto& exceptionRef)
        {
            return exceptionRef.get().code() == expectedError->code
                and (not expectedError->message.has_value()
                     or std::string_view{exceptionRef.get().what()}.find(expectedError->message.value()) != std::string_view::npos);
        });

    if (not expectedErrorOccurred)
    {
        return std::unexpected(Mismatch{fmt::format(
            "Expected error \"{}({})\" to occur, but it did not! Actual: {}",
            expectedError->message.value_or(""),
            expectedError->code,
            actual)});
    }

    return Success{};
}

void processQueryWithError(
    std::shared_ptr<RunningQuery> runningQuery,
    SystestProgressTracker& progressTracker,
    std::vector<std::shared_ptr<RunningQuery>>& failed,
    const std::optional<DistributedException>& exception,
    const QueryPerformanceMessageBuilder& performanceMessageBuilder)
{
    runningQuery->exception = exception;
    auto verdict = checkFailedQuery(runningQuery->exception, runningQuery->systestQuery.expectation);
    reportResult(runningQuery, progressTracker, failed, std::move(verdict), performanceMessageBuilder);
}

}

/// NOLINTBEGIN(readability-function-cognitive-complexity)
std::vector<RunningQuery> runQueries(
    const std::vector<SystestQuery>& queries,
    const uint64_t numConcurrentQueries,
    QuerySubmitter& querySubmitter,
    SystestProgressTracker& progressTracker,
    const QueryPerformanceMessageBuilder& queryPerformanceMessage)
{
    using SystestKey = std::pair<TestName, SystestQueryId>;
    std::unordered_set<SystestKey> completedQueries; /// Track which queries have completed

    std::queue<SystestQuery> pending;
    std::vector<SystestQuery> dependentQueries;
    for (auto it = queries.rbegin(); it != queries.rend(); ++it)
    {
        if (it->runAfter.has_value() && it->runAfter.value().second.getRawValue() != 0)
        {
            dependentQueries.push_back(*it);
        }
        else
        {
            pending.push(*it);
        }
    }

    /// Validate that all dependencies exist in queries
    for (const auto& dependentQuery : dependentQueries)
    {
        if (dependentQuery.runAfter.has_value()) [[likely]]
        {
            const auto& dependency = dependentQuery.runAfter.value();
            bool dependencyExists = false;
            for (const auto& query : queries)
            {
                if (query.testName == dependency.first && query.queryIdInFile == dependency.second)
                {
                    dependencyExists = true;
                    break;
                }
            }
            if (!dependencyExists)
            {
                throw TestException(
                    "{}:{} has nonexistent dependency {}:{}",
                    dependentQuery.testName,
                    dependentQuery.queryIdInFile,
                    dependency.first,
                    dependency.second);
            }
        }
    }

    std::unordered_map<DistributedQueryId, std::shared_ptr<RunningQuery>> active;
    std::unordered_map<DistributedQueryId, DistributedQueryStatusSnapshot> finishedDifferentialQueries;
    /// Queries whose outcome no longer matters: the other half of their differential pair already failed and was reported.
    /// Their terminal state only cleans them up, so the pair reports one verdict and the run still drains every started query.
    std::unordered_set<DistributedQueryId> discardedQueries;
    std::vector<std::shared_ptr<RunningQuery>> failed;

    const auto canRunQuery = [&completedQueries](const SystestQuery& query) -> bool
    {
        if (!query.runAfter.has_value() || (query.runAfter.has_value() && query.runAfter.value().second.getRawValue() == 0))
        {
            return true;
        }
        return completedQueries.contains(query.runAfter.value());
    };

    const auto moveDependentsToPending = [&](const SystestQuery& parentQuery) -> void
    {
        completedQueries.emplace(parentQuery.testName, parentQuery.queryIdInFile);
        /// check if any dependent query can now be run
        auto it = dependentQueries.begin();
        while (it != dependentQueries.end())
        {
            if (canRunQuery(*it))
            {
                pending.emplace(std::move(*it));
                it = dependentQueries.erase(it);
            }
            else
            {
                ++it;
            }
        }
    };

    const auto startMoreQueries = [&] -> bool
    {
        bool hasOneMoreQueryToStart = false;
        while (active.size() < numConcurrentQueries and not pending.empty())
        {
            SystestQuery nextQuery = std::move(pending.front());
            pending.pop();

            INVARIANT(
                canRunQuery(nextQuery),
                "Cannot run query from {} with the in file id {} as it's dependencies have not finished!",
                nextQuery.testName,
                nextQuery.queryIdInFile);

            if (nextQuery.actualExplainOutput.has_value())
            {
                /// EXPLAIN statements are never submitted to the worker; their output was computed at bind time,
                /// so compare it against the expected result lines and report immediately.
                auto runningQuery = std::make_shared<RunningQuery>(nextQuery);
                reportResult(runningQuery, progressTracker, failed, checkExplainedQuery(nextQuery), queryPerformanceMessage);
                moveDependentsToPending(nextQuery);
                continue;
            }

            if (nextQuery.differentialQueryPlan.has_value() and nextQuery.planInfoOrException.has_value())
            {
                /// Start both differential queries
                auto reg = querySubmitter.startQuery(nextQuery.planInfoOrException.value().queryPlan);
                auto regDiff = querySubmitter.startQuery(nextQuery.differentialQueryPlan.value());
                if (reg and regDiff)
                {
                    hasOneMoreQueryToStart = true;
                    active.emplace(*reg, std::make_shared<RunningQuery>(nextQuery, *reg, *regDiff));
                    active.emplace(*regDiff, std::make_shared<RunningQuery>(nextQuery, *regDiff, *reg));
                }
                else
                {
                    /// A half that did start still reaches a terminal state, so it gets discarded rather than checked.
                    if (reg)
                    {
                        discardedQueries.insert(*reg);
                    }
                    if (regDiff)
                    {
                        discardedQueries.insert(*regDiff);
                    }
                    auto failure = not reg ? std::move(reg).error() : std::move(regDiff).error();
                    processQueryWithError(
                        std::make_shared<RunningQuery>(nextQuery, nextQuery.planInfoOrException.value().queryPlan.getQueryId()),
                        progressTracker,
                        failed,
                        DistributedException(std::unordered_map<Host, std::vector<Exception>>{{Host("systest"), std::vector{failure}}}),
                        queryPerformanceMessage);
                    moveDependentsToPending(nextQuery);
                }
            }
            else if (nextQuery.planInfoOrException.has_value())
            {
                if (auto reg = querySubmitter.startQuery(nextQuery.planInfoOrException.value().queryPlan))
                {
                    hasOneMoreQueryToStart = true;
                    active.emplace(*reg, std::make_shared<RunningQuery>(nextQuery, *reg));
                }
                else
                {
                    processQueryWithError(
                        std::make_shared<RunningQuery>(nextQuery, nextQuery.planInfoOrException.value().queryPlan.getQueryId()),
                        progressTracker,
                        failed,
                        DistributedException(std::unordered_map<Host, std::vector<Exception>>{{Host("systest"), std::vector{reg.error()}}}),
                        queryPerformanceMessage);
                    moveDependentsToPending(nextQuery);
                }
            }
            else
            {
                /// There was an error during query parsing, report the result and don't register the query
                processQueryWithError(
                    std::make_shared<RunningQuery>(nextQuery),
                    progressTracker,
                    failed,
                    DistributedException(std::unordered_map<Host, std::vector<Exception>>{
                        {Host("systest"), std::vector{nextQuery.planInfoOrException.error()}}}),
                    queryPerformanceMessage);
                moveDependentsToPending(nextQuery);
            }
        }
        return hasOneMoreQueryToStart;
    };

    while (startMoreQueries() or not(active.empty() and pending.empty() and dependentQueries.empty()))
    {
        for (const auto& queryStatus : querySubmitter.finishedQueries())
        {
            if (discardedQueries.erase(queryStatus.queryId) > 0)
            {
                active.erase(queryStatus.queryId);
                continue;
            }

            auto it = active.find(queryStatus.queryId);
            if (it == active.end())
            {
                throw TestException("received unregistered queryId: {}", queryStatus.queryId);
            }

            auto& runningQuery = it->second;

            if (queryStatus.getGlobalQueryStatus() == DistributedQueryStatus::Failed)
            {
                processQueryWithError(it->second, progressTracker, failed, queryStatus.coalesceException(), queryPerformanceMessage);
                moveDependentsToPending(runningQuery->systestQuery);
                /// The pair has its verdict, so the other half only has to drain.
                /// A half that already finished leaves right away, and one still running gets discarded when it arrives.
                if (const auto partner = runningQuery->differentialQueryPair)
                {
                    if (finishedDifferentialQueries.erase(*partner) > 0)
                    {
                        active.erase(*partner);
                    }
                    else
                    {
                        discardedQueries.insert(*partner);
                    }
                }
                active.erase(queryStatus.queryId);
                continue;
            }

            /// Update the query summary
            runningQuery->queryStatus = queryStatus;

            /// For differential queries, check if both queries in the pair have finished
            if (runningQuery->differentialQueryPair.has_value())
            {
                /// Store this query's summary
                finishedDifferentialQueries[queryStatus.queryId] = queryStatus;

                /// Check if the other query in the pair has also finished
                const auto otherQueryId = runningQuery->differentialQueryPair.value();
                const auto otherSummaryIt = finishedDifferentialQueries.find(otherQueryId);

                if (otherSummaryIt != finishedDifferentialQueries.end())
                {
                    /// Both queries have finished, process the differential comparison
                    auto otherRunningQueryIt = active.find(otherQueryId);
                    if (otherRunningQueryIt != active.end())
                    {
                        otherRunningQueryIt->second->queryStatus = otherSummaryIt->second;
                    }

                    reportResult(
                        runningQuery, progressTracker, failed, checkSucceededQuery(runningQuery->systestQuery), queryPerformanceMessage);

                    if (otherRunningQueryIt != active.end())
                    {
                        active.erase(otherRunningQueryIt);
                    }
                    moveDependentsToPending(runningQuery->systestQuery);
                    finishedDifferentialQueries.erase(otherSummaryIt);
                    active.erase(it);
                    finishedDifferentialQueries.erase(queryStatus.queryId);
                }

                continue;
            }

            /// Regular query (not differential), process immediately
            reportResult(runningQuery, progressTracker, failed, checkSucceededQuery(runningQuery->systestQuery), queryPerformanceMessage);
            moveDependentsToPending(runningQuery->systestQuery);
            active.erase(it);
        }
    }

    auto failedViews = failed | std::views::filter(std::not_fn(passes)) | std::views::transform([](auto& p) { return *p; });
    return {failedViews.begin(), failedViews.end()};
}

/// NOLINTEND(readability-function-cognitive-complexity)

void printQueryResultToStdOut(
    const RunningQuery& runningQuery, SystestProgressTracker& progressTracker, const std::string_view queryPerformanceMessage)
{
    const auto queryCounterAsString = std::to_string(progressTracker.getQueryCounter());
    const auto progressPercent = std::clamp(progressTracker.getProgressInPercent(), 0.0, 100.0);
    const auto caseId = fmt::format(
        "{}",
        TestCaseId{
            .originFile = runningQuery.systestQuery.testName.getRawValue(),
            .queryIdInFile = runningQuery.systestQuery.queryIdInFile,
            .overrides = runningQuery.systestQuery.configurationOverride});

    const auto counterPad = padSizeQueryCounter > queryCounterAsString.size() ? padSizeQueryCounter - queryCounterAsString.size() : 0;
    std::cout << std::string(counterPad, ' ');
    std::cout << queryCounterAsString << "/" << progressTracker.getTotalQueries();
    std::cout << fmt::format(" ({:5.1f}%) ", progressPercent);
    std::cout << caseId;

    const auto paddingDots = (caseId.size() >= padSizeSuccess) ? 0 : (padSizeSuccess - caseId.size());
    const auto maxPadding = 1000;
    const auto finalPadding = std::min<size_t>(paddingDots, maxPadding);
    std::cout << std::string(finalPadding, '.');
    INVARIANT(runningQuery.verdict.has_value(), "a query is reported only after it was checked");
    if (runningQuery.verdict->has_value())
    {
        fmt::print(fmt::emphasis::bold | fg(fmt::color::green), "PASSED {}\n", queryPerformanceMessage);
    }
    else
    {
        fmt::print(fmt::emphasis::bold | fg(fmt::color::red), "FAILED {}\n", queryPerformanceMessage);
        std::cout << "===================================================================" << '\n';
        std::cout << runningQuery.systestQuery.queryDefinition << '\n';
        std::cout << "===================================================================" << '\n';
        fmt::print(fmt::emphasis::bold | fg(fmt::color::red), "Error: {}\n", runningQuery.verdict->error().detail);
        std::cout << "===================================================================" << '\n';
    }
}

std::vector<RunningQuery> runQueriesAtLocalWorker(
    const std::vector<SystestQuery>& queries,
    const uint64_t numConcurrentQueries,
    const SystestClusterConfiguration& clusterConfig,
    const SingleNodeWorkerConfiguration& configuration,
    SystestProgressTracker& progressTracker,
    const QueryPerformanceMessageBuilder& queryPerformanceMessage)
{
    auto catalog = std::make_shared<WorkerCatalog>(clusterConfig.workers);

    QuerySubmitter submitter(std::make_unique<QueryManager>(std::move(catalog), createEmbeddedBackend(configuration)));
    return runQueries(queries, numConcurrentQueries, submitter, progressTracker, queryPerformanceMessage);
}

namespace
{
/// Size and no. tuples of all input files of the query, so that the throughput can be derived from the elapsed time.
/// The list holds one entry per source reference, so a query that reads a file through two references counts it twice.
void recordProcessedInput(RunningQuery& runningQuery)
{
    size_t bytesProcessed = 0;
    size_t tuplesProcessed = 0;
    for (const auto& sourcePath : runningQuery.systestQuery.inputFiles)
    {
        if (not(std::filesystem::exists(sourcePath) and sourcePath.has_filename()))
        {
            NES_ERROR("Source path is empty or does not exist.");
            bytesProcessed = 0;
            tuplesProcessed = 0;
            break;
        }

        bytesProcessed += std::filesystem::file_size(sourcePath);

        /// Counting the lines, i.e., \n in the sourcePath
        std::ifstream inFile(sourcePath);
        tuplesProcessed += std::count(std::istreambuf_iterator(inFile), std::istreambuf_iterator<char>(), '\n');
    }
    runningQuery.bytesProcessed = bytesProcessed;
    runningQuery.tuplesProcessed = tuplesProcessed;
}
}

std::vector<RunningQuery> runQueriesAndBenchmark(
    const std::vector<SystestQuery>& queries,
    const SingleNodeWorkerConfiguration& configuration,
    std::vector<BenchmarkResult>& benchmarkResults,
    const SystestClusterConfiguration& clusterConfig,
    SystestProgressTracker& progressTracker)
{
    /// The performance message builder is invoked exactly once per query that reached the stopped state, which is exactly the set of
    /// queries that can be timed. That makes it the hook for collecting the benchmark results.
    const QueryPerformanceMessageBuilder benchmarkQuery = [&benchmarkResults](RunningQuery& runningQuery)
    {
        recordProcessedInput(runningQuery);
        const auto executionTimeInSeconds = runningQuery.getElapsedTime().count();
        benchmarkResults.push_back(
            {.queryName = runningQuery.systestQuery.testName.getRawValue(),
             .time = executionTimeInSeconds,
             .bytesPerSecond = static_cast<double>(runningQuery.bytesProcessed.value_or(NAN)) / executionTimeInSeconds,
             .tuplesPerSecond = static_cast<double>(runningQuery.tuplesProcessed.value_or(NAN)) / executionTimeInSeconds});
        return fmt::format(" in {} ({})", runningQuery.getElapsedTime(), runningQuery.getThroughput());
    };

    /// Benchmarking runs one query at a time so that the timings are not skewed by concurrently running queries.
    return runQueriesAtLocalWorker(queries, 1, clusterConfig, configuration, progressTracker, benchmarkQuery);
}

std::vector<RunningQuery> runQueriesAtRemoteWorker(
    const std::vector<SystestQuery>& queries,
    const uint64_t numConcurrentQueries,
    const SystestClusterConfiguration& clusterConfig,
    SystestProgressTracker& progressTracker,
    const QueryPerformanceMessageBuilder& queryPerformanceMessage)
{
    auto catalog = std::make_shared<WorkerCatalog>(clusterConfig.workers);

    /// Running the Systest against a remote worker setup cannot use configuration overrides as the worker configuration is not handled
    /// by the systest tool. Currently we will skip any query which has a configuration override.
    const auto queriesWithoutConfigurationOverrides
        = queries
        | std::views::filter(
              [](const auto& query)
              {
                  if (!query.configurationOverride.empty())
                  {
                      fmt::println("Skipping test {} because it has a configuration override", query.testName);
                      return false;
                  }
                  return true;
              })
        | std::ranges::to<std::vector>();

    progressTracker.setTotalQueries(queriesWithoutConfigurationOverrides.size());

    auto remoteQueryManager = std::make_unique<QueryManager>(std::move(catalog), createGRPCBackend());
    QuerySubmitter submitter(std::move(remoteQueryManager));
    return runQueries(queriesWithoutConfigurationOverrides, numConcurrentQueries, submitter, progressTracker, queryPerformanceMessage);
}

}

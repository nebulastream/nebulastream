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

#include <SystestRunner.hpp>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <expected> /// NOLINT(misc-include-cleaner)
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <optional>
#include <ostream>
#include <queue>
#include <ranges>
#include <regex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Listeners/QueryLog.hpp>
#include <QueryManager/EmbeddedWorkerQuerySubmissionBackend.hpp>
#include <QueryManager/GRPCQuerySubmissionBackend.hpp>
#include <QueryManager/QueryManager.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/URI.hpp>
#include <fmt/base.h>
#include <fmt/color.h>
#include <fmt/format.h>
#include <nlohmann/json.hpp> ///NOLINT(misc-include-cleaner)
#include <DistributedQuery.hpp>
#include <ErrorHandling.hpp>
#include <QuerySubmitter.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <SystestConfiguration.hpp>
#include <SystestResultCheck.hpp>
#include <SystestState.hpp>
#include <WorkerCatalog.hpp>
#include <WorkerConfig.hpp>

namespace NES::Systest
{
namespace
{
template <typename ErrorCallable>
void reportResult(
    std::shared_ptr<RunningQuery>& runningQuery,
    SystestProgressTracker& progressTracker,
    std::vector<std::shared_ptr<RunningQuery>>& failed,
    ErrorCallable&& errorBuilder,
    const QueryPerformanceMessageBuilder& performanceMessageBuilder)
{
    std::string msg = errorBuilder();
    runningQuery->passed = msg.empty();

    std::string performanceMessage;
    if (performanceMessageBuilder)
    {
        performanceMessage = performanceMessageBuilder(*runningQuery);
    }

    progressTracker.incrementQueryCounter();
    printQueryResultToStdOut(*runningQuery, msg, progressTracker, performanceMessage);
    if (!msg.empty())
    {
        failed.push_back(runningQuery);
    }
}

bool passes(const std::shared_ptr<RunningQuery>& runningQuery)
{
    return runningQuery->passed;
}

void processQueryWithError(
    std::shared_ptr<RunningQuery> runningQuery,
    SystestProgressTracker& progressTracker,
    std::vector<std::shared_ptr<RunningQuery>>& failed,
    const std::optional<Exception>& exception,
    const QueryPerformanceMessageBuilder& performanceMessageBuilder)
{
    runningQuery->exception = exception;
    reportResult(
        runningQuery,
        progressTracker,
        failed,
        [&]
        {
            if (std::holds_alternative<ExpectedError>(runningQuery->systestQuery.expectedResultsOrExpectedError)
                and std::get<ExpectedError>(runningQuery->systestQuery.expectedResultsOrExpectedError).code
                    == runningQuery->exception->code())
            {
                return std::string{};
            }
            return fmt::format("unexpected parsing error: {}", *runningQuery->exception);
        },
        performanceMessageBuilder);
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
    std::queue<SystestQuery> pending;
    for (auto it = queries.rbegin(); it != queries.rend(); ++it)
    {
        pending.push(*it);
    }

    std::unordered_map<DistributedQueryId, std::shared_ptr<RunningQuery>> active;
    std::unordered_map<DistributedQueryId, DistributedQueryStatus> finishedDifferentialQueries;
    std::vector<std::shared_ptr<RunningQuery>> failed;

    const auto startMoreQueries = [&] -> bool
    {
        bool hasOneMoreQueryToStart = false;
        while (active.size() < numConcurrentQueries and not pending.empty())
        {
            SystestQuery nextQuery = std::move(pending.front());
            pending.pop();

            if (nextQuery.differentialQueryPlan.has_value() and nextQuery.planInfoOrException.has_value())
            {
                /// Start both differential queries
                auto reg = querySubmitter.registerQuery(nextQuery.planInfoOrException.value().queryPlan);
                auto regDiff = querySubmitter.registerQuery(nextQuery.differentialQueryPlan.value());
                if (reg and regDiff)
                {
                    hasOneMoreQueryToStart = true;
                    querySubmitter.startQuery(*reg);
                    querySubmitter.startQuery(*regDiff);
                    active.emplace(*reg, std::make_shared<RunningQuery>(nextQuery, *reg, *regDiff));
                    active.emplace(*regDiff, std::make_shared<RunningQuery>(nextQuery, *regDiff, *reg));
                }
                else
                {
                    processQueryWithError(
                        std::make_shared<RunningQuery>(nextQuery), progressTracker, failed, {reg.error()}, queryPerformanceMessage);
                }
            }
            else if (nextQuery.planInfoOrException.has_value())
            {
                /// Registration
                if (auto reg = querySubmitter.registerQuery(nextQuery.planInfoOrException.value().queryPlan))
                {
                    hasOneMoreQueryToStart = true;
                    querySubmitter.startQuery(*reg);
                    active.emplace(*reg, std::make_shared<RunningQuery>(nextQuery, *reg));
                }
                else
                {
                    processQueryWithError(
                        std::make_shared<RunningQuery>(nextQuery), progressTracker, failed, {reg.error()}, queryPerformanceMessage);
                }
            }
            else
            {
                /// There was an error during query parsing, report the result and don't register the query
                processQueryWithError(
                    std::make_shared<RunningQuery>(nextQuery),
                    progressTracker,
                    failed,
                    {nextQuery.planInfoOrException.error()},
                    queryPerformanceMessage);
            }
        }
        return hasOneMoreQueryToStart;
    };

    while (startMoreQueries() or not(active.empty() and pending.empty()))
    {
        for (const auto& queryStatus : querySubmitter.finishedQueries())
        {
            auto it = active.find(queryStatus.queryId);
            if (it == active.end())
            {
                throw TestException("received unregistered queryId: {}", queryStatus.queryId);
            }

            auto& runningQuery = it->second;

            if (queryStatus.getGlobalQueryState() == DistributedQueryState::Failed)
            {
                processQueryWithError(it->second, progressTracker, failed, queryStatus.coalesceException(), queryPerformanceMessage);
                active.erase(it);
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
                        runningQuery,
                        progressTracker,
                        failed,
                        [&]
                        {
                            if (std::holds_alternative<ExpectedError>(runningQuery->systestQuery.expectedResultsOrExpectedError))
                            {
                                return fmt::format(
                                    "expected error {} but query succeeded",
                                    std::get<ExpectedError>(runningQuery->systestQuery.expectedResultsOrExpectedError).code);
                            }
                            if (auto err = checkResult(*runningQuery))
                            {
                                return *err;
                            }
                            return std::string{};
                        },
                        queryPerformanceMessage);

                    if (otherRunningQueryIt != active.end())
                    {
                        active.erase(otherRunningQueryIt);
                    }
                    finishedDifferentialQueries.erase(otherSummaryIt);
                    active.erase(it);
                    finishedDifferentialQueries.erase(queryStatus.queryId);
                }

                continue;
            }

            /// Regular query (not differential), process immediately
            reportResult(
                runningQuery,
                progressTracker,
                failed,
                [&]
                {
                    if (std::holds_alternative<ExpectedError>(runningQuery->systestQuery.expectedResultsOrExpectedError))
                    {
                        return fmt::format(
                            "expected error {} but query succeeded",
                            std::get<ExpectedError>(runningQuery->systestQuery.expectedResultsOrExpectedError).code);
                    }
                    if (auto err = checkResult(*runningQuery))
                    {
                        return *err;
                    }
                    return std::string{};
                },
                queryPerformanceMessage);
            active.erase(it);
        }
    }

    auto failedViews = failed | std::views::filter(std::not_fn(passes)) | std::views::transform([](auto& p) { return *p; });
    return {failedViews.begin(), failedViews.end()};
}

/// NOLINTEND(readability-function-cognitive-complexity)

namespace
{
std::vector<RunningQuery> serializeExecutionResults(const std::vector<RunningQuery>& queries, nlohmann::json& resultJson)
{
    std::vector<RunningQuery> failedQueries;
    for (const auto& queryRan : queries)
    {
        if (!queryRan.passed)
        {
            failedQueries.emplace_back(queryRan);
        }
        const auto executionTimeInSeconds = queryRan.getElapsedTime().count();
        resultJson.push_back({
            {"query name", queryRan.systestQuery.testName},
            {"time", executionTimeInSeconds},
            {"bytesPerSecond", static_cast<double>(queryRan.bytesProcessed.value_or(NAN)) / executionTimeInSeconds},
            {"tuplesPerSecond", static_cast<double>(queryRan.tuplesProcessed.value_or(NAN)) / executionTimeInSeconds},
        });
    }
    return failedQueries;
}
}

std::vector<RunningQuery> runQueriesAndBenchmark(
    const std::vector<SystestQuery>& queries,
    const SingleNodeWorkerConfiguration& configuration,
    nlohmann::json& resultJson,
    const SystestClusterConfiguration& clusterConfig,
    SystestProgressTracker& progressTracker)
{
    auto catalog = std::make_shared<WorkerCatalog>();
    for (const auto& [host, grpc, capacity, downstream] : clusterConfig.workers)
    {
        catalog->addWorker(HostAddr(host), GrpcAddr(grpc), capacity, downstream);
    }

    auto worker = std::make_unique<QueryManager>(std::move(catalog), createEmbeddedBackend(configuration));
    QuerySubmitter submitter(std::move(worker));
    std::vector<std::shared_ptr<RunningQuery>> ranQueries;
    progressTracker.reset();
    progressTracker.setTotalQueries(queries.size());
    for (const auto& queryToRun : queries)
    {
        if (not queryToRun.planInfoOrException.has_value())
        {
            NES_ERROR("skip failing query: {}", queryToRun.testName);
            continue;
        }

        const auto registrationResult = submitter.registerQuery(queryToRun.planInfoOrException.value().queryPlan);
        if (not registrationResult.has_value())
        {
            NES_ERROR("skip failing query: {}", queryToRun.testName);
            continue;
        }
        auto queryId = registrationResult.value();

        auto runningQueryPtr = std::make_shared<RunningQuery>(queryToRun, queryId);
        runningQueryPtr->passed = false;
        ranQueries.emplace_back(runningQueryPtr);
        submitter.startQuery(queryId);
        const auto summary = submitter.finishedQueries().at(0);

        if (summary.getGlobalQueryState() == DistributedQueryState::Failed)
        {
            NES_ERROR("Query {} has failed with: {}", queryId, summary.coalesceException());
            continue;
        }

        if (summary.getGlobalQueryState() != DistributedQueryState::Stopped)
        {
            NES_ERROR("Query {} terminated in unexpected state {}", queryId, summary.getGlobalQueryState());
            continue;
        }

        runningQueryPtr->queryStatus = summary;

        /// Getting the size and no. tuples of all input files to pass this information to currentRunningQuery.bytesProcessed
        size_t bytesProcessed = 0;
        size_t tuplesProcessed = 0;
        for (const auto& [sourcePath, sourceOccurrencesInQuery] :
             queryToRun.planInfoOrException.value().sourcesToFilePathsAndCounts | std::views::values)
        {
            if (not(std::filesystem::exists(sourcePath.getRawValue()) and sourcePath.getRawValue().has_filename()))
            {
                NES_ERROR("Source path is empty or does not exist.");
                bytesProcessed = 0;
                tuplesProcessed = 0;
                break;
            }

            bytesProcessed += (std::filesystem::file_size(sourcePath.getRawValue()) * sourceOccurrencesInQuery);

            /// Counting the lines, i.e., \n in the sourcePath
            std::ifstream inFile(sourcePath.getRawValue());
            tuplesProcessed
                += std::count(std::istreambuf_iterator(inFile), std::istreambuf_iterator<char>(), '\n') * sourceOccurrencesInQuery;
        }
        ranQueries.back()->bytesProcessed = bytesProcessed;
        ranQueries.back()->tuplesProcessed = tuplesProcessed;

        auto errorMessage = checkResult(*ranQueries.back());
        ranQueries.back()->passed = not errorMessage.has_value();
        const auto queryPerformanceMessage
            = fmt::format(" in {} ({})", ranQueries.back()->getElapsedTime(), ranQueries.back()->getThroughput());
        progressTracker.incrementQueryCounter();
        printQueryResultToStdOut(*ranQueries.back(), errorMessage.value_or(""), progressTracker, queryPerformanceMessage);
    }

    return serializeExecutionResults(
        ranQueries | std::views::transform([](const auto& query) { return *query; }) | std::ranges::to<std::vector>(), resultJson);
}

void printQueryResultToStdOut(
    const RunningQuery& runningQuery,
    const std::string& errorMessage,
    SystestProgressTracker& progressTracker,
    const std::string_view queryPerformanceMessage)
{
    const auto queryNameLength = runningQuery.systestQuery.testName.size();
    const auto queryNumberAsString = runningQuery.systestQuery.queryIdInFile.toString();
    const auto queryNumberLength = queryNumberAsString.size();
    const auto queryCounterAsString = std::to_string(progressTracker.getQueryCounter());
    const auto progressPercent = std::clamp(progressTracker.getProgressInPercent(), 0.0, 100.0);

    std::string overrideStr;
    if (not runningQuery.systestQuery.configurationOverride.overrideParameters.empty())
    {
        std::vector<std::string> kvs;
        kvs.reserve(runningQuery.systestQuery.configurationOverride.overrideParameters.size());
        for (const auto& [key, value] : runningQuery.systestQuery.configurationOverride.overrideParameters)
        {
            kvs.push_back(fmt::format("{}={}", key, value));
        }
        overrideStr = fmt::format(" [{}]", fmt::join(kvs, ", "));
    }
    const auto counterPad = padSizeQueryCounter > queryCounterAsString.size() ? padSizeQueryCounter - queryCounterAsString.size() : 0;
    std::cout << std::string(counterPad, ' ');
    std::cout << queryCounterAsString << "/" << progressTracker.getTotalQueries();
    std::cout << fmt::format(" ({:5.1f}%) ", progressPercent);
    const auto numberPad = padSizeQueryNumber > queryNumberLength ? padSizeQueryNumber - queryNumberLength : 0;
    std::cout << runningQuery.systestQuery.testName << ":" << std::string(numberPad, '0') << queryNumberAsString;
    std::cout << overrideStr;

    const auto totalUsedSpace = queryNameLength + padSizeQueryNumber + overrideStr.size();
    const auto paddingDots = (totalUsedSpace >= padSizeSuccess) ? 0 : (padSizeSuccess - totalUsedSpace);
    const auto maxPadding = 1000;
    const auto finalPadding = std::min<size_t>(paddingDots, maxPadding);
    std::cout << std::string(finalPadding, '.');
    if (runningQuery.passed)
    {
        fmt::print(fmt::emphasis::bold | fg(fmt::color::green), "PASSED {}\n", queryPerformanceMessage);
    }
    else
    {
        fmt::print(fmt::emphasis::bold | fg(fmt::color::red), "FAILED {}\n", queryPerformanceMessage);
        std::cout << "===================================================================" << '\n';
        std::cout << runningQuery.systestQuery.queryDefinition << '\n';
        std::cout << "===================================================================" << '\n';
        fmt::print(fmt::emphasis::bold | fg(fmt::color::red), "Error: {}\n", errorMessage);
        std::cout << "===================================================================" << '\n';
    }
}

std::vector<RunningQuery> runQueriesAtLocalWorker(
    const std::vector<SystestQuery>& queries,
    const uint64_t numConcurrentQueries,
    const SystestClusterConfiguration& clusterConfig,
    const SingleNodeWorkerConfiguration& configuration,
    SystestProgressTracker& progressTracker)
{
    auto catalog = std::make_shared<WorkerCatalog>();
    for (const auto& [host, grpc, capacity, downstream] : clusterConfig.workers)
    {
        catalog->addWorker(HostAddr(host), GrpcAddr(grpc), capacity, downstream);
    }

    QuerySubmitter submitter(std::make_unique<QueryManager>(std::move(catalog), createEmbeddedBackend(configuration)));
    return runQueries(queries, numConcurrentQueries, submitter, progressTracker, discardPerformanceMessage);
}

std::vector<RunningQuery> runQueriesAtRemoteWorker(
    const std::vector<SystestQuery>& queries,
    const uint64_t numConcurrentQueries,
    const SystestClusterConfiguration& clusterConfig,
    SystestProgressTracker& progressTracker)
{
    auto catalog = std::make_shared<WorkerCatalog>();
    for (const auto& [host, grpc, capacity, downstream] : clusterConfig.workers)
    {
        catalog->addWorker(HostAddr(host), GrpcAddr(grpc), capacity, downstream);
    }

    /// Running the Systest against a remote worker setup cannot use configuration overrides as the worker configuration is not handled
    /// by the systest tool. Currently we will skip any query which has a configuration override.
    const auto queriesWithoutConfigurationOverrides
        = queries
        | std::views::filter(
              [](const auto& query)
              {
                  if (!query.configurationOverride.overrideParameters.empty())
                  {
                      fmt::println("Skipping test {} because it is has a configuration override", query.testName);
                      return false;
                  }
                  return true;
              })
        | std::ranges::to<std::vector>();

    auto remoteQueryManager = std::make_unique<QueryManager>(std::move(catalog), createGRPCBackend());
    QuerySubmitter submitter(std::move(remoteQueryManager));
    return runQueries(queriesWithoutConfigurationOverrides, numConcurrentQueries, submitter, progressTracker, discardPerformanceMessage);
}

}

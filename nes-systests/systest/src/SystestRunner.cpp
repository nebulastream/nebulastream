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
#include <exception>
#include <expected> /// NOLINT(misc-include-cleaner)
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <queue>
#include <ranges>
#include <regex>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>
#include <fmt/base.h>
#include <fmt/color.h>
#include <fmt/format.h>
#include <fmt/ostream.h>
#include <fmt/ranges.h>
#include <folly/MPMCQueue.h>
#include <nlohmann/json.hpp>
#include <scope_guard.hpp>


#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <QueryManager/EmbeddedWorkerQueryManager.hpp>
#include <QueryManager/ForkedWorkerQueryManager.hpp>
#include <QueryManager/GRPCQueryManager.hpp>
#include <Runtime/Execution/QueryStatus.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Strings.hpp>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <nlohmann/json_fwd.hpp>
#include <ErrorHandling.hpp>
#include <InlineEventServer.hpp>
#include <QuerySubmitter.hpp>
#include <SingleNodeWorker.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <SystestParser.hpp>
#include <SystestResultCheck.hpp>
#include <SystestState.hpp>

namespace NES::Systest
{
namespace
{
constexpr auto InlineEventQueryTimeout = std::chrono::seconds{45};

void stopInlineEventControllers(const SystestQuery& query)
{
    for (const auto& [_, controller] : query.inlineEventControllers)
    {
        if (controller)
        {
            controller->stop();
        }
    }
}

std::vector<std::shared_ptr<InlineEventController>> collectInlineEventControllers(const std::vector<SystestQuery>& queries)
{
    std::unordered_set<const InlineEventController*> seen;
    std::vector<std::shared_ptr<InlineEventController>> controllers;
    controllers.reserve(queries.size());
    for (const auto& query : queries)
    {
        for (const auto& [_, controller] : query.inlineEventControllers)
        {
            if (controller && seen.insert(controller.get()).second)
            {
                controllers.push_back(controller);
            }
        }
    }
    return controllers;
}

void stopInlineEventControllers(const std::vector<std::shared_ptr<InlineEventController>>& controllers)
{
    for (const auto& controller : controllers)
    {
        if (controller)
        {
            controller->stop();
        }
    }
}

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

class WorkerLifecycle
{
public:
    WorkerLifecycle(LogicalPlan plan, SingleNodeWorkerConfiguration config, std::function<void()> restartCleanup)
        : plan(std::move(plan)), configuration(std::move(config)), onRestart(std::move(restartCleanup))
    {
    }

    std::expected<QueryId, Exception> start() { return startNewWorkerWithRetry(); }

    void powerOff()
    {
        std::shared_ptr<ForkedWorkerQueryManager> currentManager;
        {
            std::scoped_lock lock(mutex);
            currentManager = std::move(workerManager);
            currentQueryId.reset();
        }
        if (currentManager)
        {
            currentManager->crashWorker();
        }
    }

    std::expected<QueryId, Exception> restart()
    {
        powerOff();
        if (onRestart)
        {
            onRestart();
        }
        return startNewWorkerWithRetry();
    }

    std::expected<LocalQueryStatus, Exception> status() const
    {
        std::scoped_lock lock(mutex);
        if (!workerManager || !currentQueryId.has_value())
        {
            return std::unexpected{TestException("Worker not running for status check.")};
        }
        return workerManager->status(*currentQueryId);
    }

    bool running() const
    {
        std::scoped_lock lock(mutex);
        return workerManager != nullptr && currentQueryId.has_value();
    }

    void shutdown()
    {
        std::shared_ptr<ForkedWorkerQueryManager> currentManager;
        std::optional<QueryId> queryIdCopy;
        {
            std::scoped_lock lock(mutex);
            currentManager = std::move(workerManager);
            queryIdCopy = currentQueryId;
            currentQueryId.reset();
        }
        if (currentManager && queryIdCopy.has_value())
        {
            auto stopped = currentManager->stop(*queryIdCopy);
            if (!stopped.has_value())
            {
                NES_WARNING("Failed to stop query {} during shutdown: {}", *queryIdCopy, stopped.error());
            }
            auto unregistered = currentManager->unregister(*queryIdCopy);
            if (!unregistered.has_value())
            {
                NES_WARNING("Failed to unregister query {} during shutdown: {}", *queryIdCopy, unregistered.error());
            }
        }
    }

private:
    std::expected<QueryId, Exception> startNewWorker()
    {
        std::shared_ptr<ForkedWorkerQueryManager> newManager;
        CPPTRACE_TRY
        {
            newManager = std::make_shared<ForkedWorkerQueryManager>(configuration);
        }
        CPPTRACE_CATCH(const Exception& e)
        {
            return std::unexpected(e);
        }
        CPPTRACE_CATCH_ALT(const std::exception& e)
        {
            return std::unexpected(TestException("Failed to start worker: {}", e.what()));
        }
        CPPTRACE_CATCH_ALT(...)
        {
            return std::unexpected(TestException("Failed to start worker due to unknown error."));
        }
        auto registration = newManager->registerQuery(plan);
        if (!registration.has_value())
        {
            return std::unexpected{registration.error()};
        }
        auto started = newManager->start(*registration);
        if (!started.has_value())
        {
            return std::unexpected{started.error()};
        }
        {
            std::scoped_lock lock(mutex);
            workerManager = std::move(newManager);
            currentQueryId = *registration;
        }
        return currentQueryId.value();
    }

    std::expected<QueryId, Exception>
    startNewWorkerWithRetry(const uint32_t maxAttempts = 3, std::chrono::milliseconds backoff = std::chrono::milliseconds{100})
    {
        std::expected<QueryId, Exception> lastError = std::unexpected(TestException("Uninitialized"));
        for (uint32_t attempt = 0; attempt < maxAttempts; ++attempt)
        {
            auto started = startNewWorker();
            if (started.has_value())
            {
                return started;
            }
            lastError = started;
            std::this_thread::sleep_for(backoff);
        }
        return lastError;
    }

    LogicalPlan plan;
    SingleNodeWorkerConfiguration configuration;
    std::function<void()> onRestart;
    mutable std::mutex mutex;
    std::shared_ptr<ForkedWorkerQueryManager> workerManager;
    std::optional<QueryId> currentQueryId;
};

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
            if (exception.has_value() && std::holds_alternative<ExpectedError>(runningQuery->systestQuery.expectedResultsOrExpectedError)
                && std::get<ExpectedError>(runningQuery->systestQuery.expectedResultsOrExpectedError).code == exception->code())
            {
                return std::string{};
            }
            if (exception.has_value())
            {
                return fmt::format("unexpected parsing error: {}", *exception);
            }
            return std::string{"unexpected parsing error without exception details"};
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
    const auto inlineControllers = collectInlineEventControllers(queries);
    SCOPE_EXIT
    {
        stopInlineEventControllers(inlineControllers);
    };

    std::queue<SystestQuery> pending;
    for (auto it = queries.rbegin(); it != queries.rend(); ++it)
    {
        pending.push(*it);
    }

    std::unordered_map<QueryId, std::shared_ptr<RunningQuery>> active;
    std::unordered_map<QueryId, LocalQueryStatus> finishedDifferentialQueries;
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

            if (queryStatus.state == QueryState::Failed)
            {
                INVARIANT(queryStatus.metrics.error.has_value(), "A query that failed must have a corresponding error.");
                processQueryWithError(it->second, progressTracker, failed, queryStatus.metrics.error, queryPerformanceMessage);
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
    SystestProgressTracker& progressTracker)
{
    auto worker = std::make_unique<EmbeddedWorkerQueryManager>(configuration);
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

        if (summary.state == QueryState::Failed)
        {
            if (summary.metrics.error.has_value())
            {
                NES_ERROR("Query {} has failed with: {}", queryId, summary.metrics.error->what());
            }
            else
            {
                NES_ERROR("Query {} has failed without additional error details.", queryId);
            }
            continue;
        }

        if (summary.state != QueryState::Stopped)
        {
            NES_ERROR("Query {} terminated in unexpected state {}", queryId, summary.state);
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

namespace
{
std::expected<LocalQueryStatus, Exception> runSingleInlineRestartableQuery(
    const SystestQuery& query,
    const SingleNodeWorkerConfiguration& configuration,
    const LogicalPlan& plan,
    const std::filesystem::path& resultFile)
{
    auto cleanupResults = [&resultFile]()
    {
        std::error_code removeError;
        std::filesystem::remove(resultFile, removeError);
    };

    WorkerLifecycle lifecycle(plan, configuration, cleanupResults);

    std::atomic_bool restartFailed{false};
    std::mutex restartErrorMutex;
    std::optional<Exception> restartError;

    for (const auto& [_, controller] : query.inlineEventControllers)
    {
        if (controller)
        {
            controller->setLifecycleCallbacks(
                [&lifecycle]() { lifecycle.powerOff(); },
                [&lifecycle, &restartFailed, &restartError, &restartErrorMutex]()
                {
                    if (auto restarted = lifecycle.restart(); !restarted.has_value())
                    {
                        NES_WARNING("Failed to restart worker during inline event script: {}", restarted.error());
                        {
                            std::scoped_lock lock(restartErrorMutex);
                            restartError = restarted.error();
                        }
                        restartFailed.store(true);
                    }
                });
        }
    }

    for (const auto& [_, controller] : query.inlineEventControllers)
    {
        if (controller)
        {
            controller->restart(true);
        }
    }

    auto started = lifecycle.start();
    if (!started.has_value())
    {
        stopInlineEventControllers(query);
        lifecycle.shutdown();
        return std::unexpected(started.error());
    }

    const auto startTime = std::chrono::steady_clock::now();
    auto lastLogTime = startTime;
    while (true)
    {
        if (std::chrono::steady_clock::now() - startTime > InlineEventQueryTimeout)
        {
            stopInlineEventControllers(query);
            lifecycle.shutdown();
            return std::unexpected(TestException("Inline event query timed out while waiting for completion."));
        }

        if (restartFailed.load())
        {
            stopInlineEventControllers(query);
            std::optional<Exception> restartCopy;
            {
                std::scoped_lock lock(restartErrorMutex);
                restartCopy = restartError;
            }
            if (restartCopy.has_value())
            {
                lifecycle.shutdown();
                return std::unexpected(restartCopy.value());
            }
            lifecycle.shutdown();
            return std::unexpected(TestException("Failed to restart worker during inline event script."));
        }

        auto status = lifecycle.status();
        if (!status.has_value())
        {
            if (!lifecycle.running())
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(25));
                continue;
            }
            stopInlineEventControllers(query);
            lifecycle.shutdown();
            return std::unexpected(status.error());
        }

        if (status->state == QueryState::Failed || status->state == QueryState::Stopped)
        {
            stopInlineEventControllers(query);
            lifecycle.shutdown();
            return status;
        }
        const auto now = std::chrono::steady_clock::now();
        if (now - lastLogTime > std::chrono::seconds{1})
        {
            NES_INFO("Inline event query {} still running with state {}", query.testName, status->state);
            lastLogTime = now;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }

    /// Unreachable, but keep lifecycle cleanup for completeness.
    lifecycle.shutdown();
}
}

bool hasInlineEventWorkerRestart(const SystestQuery& query)
{
    return std::ranges::any_of(
        query.inlineEventScripts | std::views::values,
        [](const InlineEventScript& script)
        {
            return std::ranges::any_of(
                script.actions,
                [](const InlineAction& action)
                { return action.type == InlineActionType::CrashWorker || action.type == InlineActionType::RestartWorker; });
        });
}

std::vector<RunningQuery> runInlineEventQueriesWithWorkerRestart(
    const std::vector<SystestQuery>& queries, const SingleNodeWorkerConfiguration& configuration, SystestProgressTracker& progressTracker)
{
    std::vector<std::shared_ptr<RunningQuery>> failed;
    for (const auto& query : queries)
    {
        auto runningQuery = std::make_shared<RunningQuery>(query);
        if (!query.planInfoOrException.has_value())
        {
            stopInlineEventControllers(query);
            processQueryWithError(runningQuery, progressTracker, failed, {query.planInfoOrException.error()}, discardPerformanceMessage);
            continue;
        }

        const auto removeResultIfExists = [](const std::filesystem::path& path)
        {
            std::error_code ec;
            std::filesystem::remove(path, ec);
        };
        removeResultIfExists(query.resultFile());
        if (query.differentialQueryPlan.has_value())
        {
            removeResultIfExists(query.resultFileForDifferentialQuery());
        }

        auto runPlan = [&](const LogicalPlan& plan, const std::filesystem::path& resultFile)
        { return runSingleInlineRestartableQuery(query, configuration, plan, resultFile); };

        auto statusOrError = runPlan(query.planInfoOrException.value().queryPlan, query.resultFile());
        if (!statusOrError.has_value())
        {
            processQueryWithError(runningQuery, progressTracker, failed, {statusOrError.error()}, discardPerformanceMessage);
            continue;
        }
        if (statusOrError->state == QueryState::Failed)
        {
            processQueryWithError(runningQuery, progressTracker, failed, statusOrError->metrics.error, discardPerformanceMessage);
            continue;
        }

        LocalQueryStatus finalStatus = statusOrError.value();

        if (query.differentialQueryPlan.has_value())
        {
            auto differentialStatus = runPlan(query.differentialQueryPlan.value(), query.resultFileForDifferentialQuery());
            if (!differentialStatus.has_value())
            {
                processQueryWithError(runningQuery, progressTracker, failed, {differentialStatus.error()}, discardPerformanceMessage);
                continue;
            }
            if (differentialStatus->state == QueryState::Failed)
            {
                processQueryWithError(runningQuery, progressTracker, failed, differentialStatus->metrics.error, discardPerformanceMessage);
                continue;
            }
            finalStatus = differentialStatus.value();
        }

        runningQuery->queryStatus = finalStatus;
        runningQuery->queryId = finalStatus.queryId;

        reportResult(
            runningQuery,
            progressTracker,
            failed,
            [&]
            {
                if (std::holds_alternative<ExpectedError>(query.expectedResultsOrExpectedError))
                {
                    return fmt::format(
                        "expected error {} but query succeeded", std::get<ExpectedError>(query.expectedResultsOrExpectedError).code);
                }
                if (auto err = checkResult(*runningQuery))
                {
                    return *err;
                }
                return std::string{};
            },
            discardPerformanceMessage);
    }

    std::vector<RunningQuery> failedQueries;
    failedQueries.reserve(failed.size());
    for (const auto& entry : failed)
    {
        failedQueries.emplace_back(*entry);
    }
    return failedQueries;
}

std::vector<RunningQuery> runQueriesAtLocalWorker(
    const std::vector<SystestQuery>& queries,
    const uint64_t numConcurrentQueries,
    const SingleNodeWorkerConfiguration& configuration,
    SystestProgressTracker& progressTracker)
{
    auto embeddedQueryManager = std::make_unique<EmbeddedWorkerQueryManager>(configuration);
    QuerySubmitter submitter(std::move(embeddedQueryManager));
    return runQueries(queries, numConcurrentQueries, submitter, progressTracker, discardPerformanceMessage);
}

std::vector<RunningQuery> runQueriesAtRemoteWorker(
    const std::vector<SystestQuery>& queries,
    const uint64_t numConcurrentQueries,
    const URI& serverURI,
    SystestProgressTracker& progressTracker)
{
    auto remoteQueryManager = std::make_unique<GRPCQueryManager>(CreateChannel(serverURI.toString(), grpc::InsecureChannelCredentials()));
    QuerySubmitter submitter(std::move(remoteQueryManager));
    return runQueries(queries, numConcurrentQueries, submitter, progressTracker, discardPerformanceMessage);
}

}

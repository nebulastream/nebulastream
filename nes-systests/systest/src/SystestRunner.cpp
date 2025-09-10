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

#include <chrono>
#include <csignal>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

#include <fmt/format.h>

#include <Listeners/QueryLog.hpp>
#include <QueryManager/EmbeddedWorkerQuerySubmissionBackend.hpp>
#include <QueryManager/GRPCQuerySubmissionBackend.hpp>
#include <Runtime/Execution/QueryStatus.hpp>
#include <Util/Overloaded.hpp>
#include <YAML/YamlLoader.hpp>

#include <BenchmarkUtils.hpp>
#include <ErrorHandling.hpp>
#include <QueryConfig.hpp>
#include <SystestResultCheck.hpp>
#include <SystestState.hpp>
#include <WorkerConfig.hpp>

namespace NES::Systest
{
using namespace std::chrono_literals;

namespace
{
UniquePtr<QuerySubmissionBackend> createSubmissionBackend(const SystestRunner::ExecutionMode& executionMode)
{
    return std::visit(
        Overloaded{
            [](const SystestRunner::LocalExecution& local) -> UniquePtr<QuerySubmissionBackend>
            {
                return std::make_unique<EmbeddedWorkerQuerySubmissionBackend>(
                    std::views::all(local.topology.nodes)
                        | std::views::transform(
                            [](const CLI::WorkerConfig& worker) -> WorkerConfig
                            { return WorkerConfig{worker.host, worker.grpc, worker.capacity, worker.downstreamNodes}; })
                        | std::ranges::to<std::vector>(),
                    local.singleNodeWorkerConfig);
            },
            [](const SystestRunner::DistributedExecution& distributed) -> UniquePtr<QuerySubmissionBackend>
            {
                return std::make_unique<GRPCQuerySubmissionBackend>(
                    std::views::all(distributed.topology.nodes)
                    | std::views::transform(
                        [](const CLI::WorkerConfig& worker) -> WorkerConfig
                        { return WorkerConfig{worker.host, worker.grpc, worker.capacity, worker.downstreamNodes}; })
                    | std::ranges::to<std::vector>());
            }},
        executionMode);
}
}

SystestRunner::SystestRunner(std::vector<PlannedQuery> inputQueries, ExecutionMode executionMode, const SystestRunner::RunnerConfig& config)
    : queryTracker{std::move(inputQueries), config.queryConcurrency, config.shuffle}
    , endless{config.endless}
    , executionMode{executionMode}
    , queryManager(createSubmissionBackend(executionMode))
    , reporter{std::make_unique<QueryResultReporter>(queryTracker.getTotalQueries())}
{
}

SystestRunner SystestRunner::from(std::vector<PlannedQuery> queries, ExecutionMode executionMode, const SystestRunner::RunnerConfig& config)
{
    return SystestRunner{std::move(queries), std::move(executionMode), config};
}

std::atomic_bool continueSubmittingQueries = false;

namespace
{
void signalHandler(int)
{
    continueSubmittingQueries = false;
}
}

std::vector<FailedQuery> SystestRunner::run(nlohmann::json* _Nullable benchmarkResults) &&
{
    if (endless)
    {
        continueSubmittingQueries = true;
        signal(SIGINT, signalHandler);
        signal(SIGTERM, signalHandler);
    }

    do
    {
        while (not queryTracker.done())
        {
            if (!submit())
                handle();
        }
    } while (continueSubmittingQueries && queryTracker.nextIteration());

    if (benchmarkResults)
    {
        serializeBenchmarkResults(finishedQueries, *benchmarkResults);
        benchmarkResults->at("failedQueries") = reportedFailures.size();
        benchmarkResults->at("totalQueries") = benchmarkResults->at("totalQueries").get<size_t>() + reportedFailures.size();
    }

    return reportedFailures;
}

/// Submit as many queries as possible to the execution backend until either the max concurrency level is reached
/// or there are no more queries to submit.
/// Queries that have failed during planning are marked as failed and ignored.
/// Same thing applies to queries that fail during deployment.
bool SystestRunner::submit()
{
    bool atLeastOneQueryWasSubmitted = false;
    while (auto planned = queryTracker.nextPending())
    {
        auto planningResult = planned->planInfoOrException;
        planningResult
            .and_then(
                [this, &planned](const auto& planInfo) mutable
                {
                    return queryManager.registerQuery(planInfo.plan)
                        .transform(
                            [&planned](auto&& query) mutable
                            {
                                auto submitted = SubmittedQuery{std::forward<decltype(query)>(query), std::move(*planned)};
                                if (auto metrics = calculateSourceMetrics(submitted.planInfo))
                                {
                                    const auto [bytesProcessed, tuplesProcessed] = *metrics;
                                    submitted.bytesProcessed = bytesProcessed;
                                    submitted.tuplesProcessed = tuplesProcessed;
                                }
                                return submitted;
                            });
                })
            .and_then(
                [this, &atLeastOneQueryWasSubmitted](auto&& submitted) -> std::expected<void, Exception>
                {
                    return queryManager.start(submitted.query)
                        .transform([this, &submitted] { queryTracker.moveToSubmitted(std::forward<decltype(submitted)>(submitted)); });
                    atLeastOneQueryWasSubmitted = true;
                })
            .or_else(
                [this, &planned](const Exception& exception)
                {
                    onFailure<PlannedQuery>(std::move(*planned), {exception});
                    return std::expected<void, Exception>{};
                });
    }
    return atLeastOneQueryWasSubmitted;
}

/// Consume from submitted queries and request their status from the execution backend.
/// If failed/stopped, report/check result, otherwise put back into running and continue
void SystestRunner::handle()
{
    while (auto submittedQuery = queryTracker.nextSubmitted())
    {
        if (submittedQuery->submittedAt + std::chrono::seconds(100) < std::chrono::system_clock::now())
        {
            fmt::println(std::cerr, "Query {} has reached timeout", submittedQuery->ctx.testName);
            std::exit(EXIT_FAILURE);
        }

        queryManager
            .status(submittedQuery->query) /// NOLINT(*-unused-return-value)
            .and_then(
                [this, &submittedQuery](auto&& status) -> std::expected<void, std::vector<Exception>>
                {
                    switch (status.getGlobalQueryState())
                    {
                        case QueryState::Failed: {
                            /// Best effort error handling by terminating other local queries
                            const auto stopResult = queryManager.stop(submittedQuery->query);
                            if (!stopResult)
                            {
                                fmt::println(
                                    std::cerr,
                                    "Could not stop remaining queries after individual subquery failure: {}",
                                    fmt::join(stopResult.error(), ", "));
                            }
                            onFailure<SubmittedQuery>(std::move(*submittedQuery), status.getExceptions());
                            break;
                        }
                        case QueryState::Stopped:
                            onStopped(std::move(*submittedQuery), status);
                            break;
                        default:
                            queryTracker.moveToSubmitted(std::move(*submittedQuery));
                            std::this_thread::sleep_for(25ms);
                            break;
                    }
                    return {};
                })
            .or_else(
                [this, &submittedQuery](auto&& error) -> std::expected<void, std::vector<Exception>>
                {
                    onFailure<SubmittedQuery>(std::move(*submittedQuery), std::vector{error});
                    return {};
                });
    }
}

void SystestRunner::onStopped(SubmittedQuery&& submitted, const DistributedQueryStatus& queryStatus)
{
    /// Check if we expected an error but got success
    if (std::holds_alternative<ExpectedError>(submitted.ctx.expectedResultsOrError))
    {
        const auto& expectedError = std::get<ExpectedError>(submitted.ctx.expectedResultsOrError);
        reporter->reportUnexpectedSuccess(submitted.ctx, expectedError);
        auto failedQuery = FailedQuery{std::move(submitted.ctx), {}};
        reportedFailures.push_back(failedQuery);
        queryTracker.moveToFailed(std::move(failedQuery));
        return;
    }

    /// Check query results
    if (const auto errorMessage = checkResult(submitted))
    {
        auto failedQuery = FailedQuery{std::move(submitted.ctx), {}};
        reporter->reportFailure(failedQuery.ctx, *errorMessage);
        reportedFailures.push_back(failedQuery);
        queryTracker.moveToFailed(std::move(failedQuery));
        return;
    }

    const auto elapsedTime = extractElapsedTime(queryStatus);
    std::optional<std::string> throughput;

    if (elapsedTime.has_value() and submitted.bytesProcessed.has_value() and submitted.tuplesProcessed.has_value())
    {
        throughput = calculateThroughput(*submitted.bytesProcessed, *submitted.tuplesProcessed, *elapsedTime);
    }

    auto finished = FinishedQuery{std::move(submitted)};
    finished.elapsedTime = elapsedTime;
    finished.throughput = throughput;
    finishedQueries.push_back(finished);

    /// Report with performance information
    if (elapsedTime && throughput)
    {
        const auto performanceMessage = fmt::format(" in {:.3f}s ({})", elapsedTime->count(), *throughput);
        reporter->reportSuccess(finished.ctx, performanceMessage);
    }
    else if (elapsedTime)
    {
        const auto performanceMessage = fmt::format(" in {:.3f}s", elapsedTime->count());
        reporter->reportSuccess(finished.ctx, performanceMessage);
    }
    else
    {
        reporter->reportSuccess(finished.ctx, " (no timing data)");
    }

    queryTracker.moveToFinished(std::move(finished));
}
}

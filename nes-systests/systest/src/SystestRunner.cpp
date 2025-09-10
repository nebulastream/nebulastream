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
                const auto topologyPath = local.topologyPath.value_or(TEST_CONFIGURATION_DIR "/topologies/default_distributed.yaml");
                auto [nodes] = CLI::YamlLoader<CLI::ClusterConfig>::load(topologyPath);
                return std::make_unique<EmbeddedWorkerQuerySubmissionBackend>(
                    std::views::all(nodes)
                        | std::views::transform(
                            [](const CLI::WorkerConfig& worker) -> WorkerConfig
                            { return WorkerConfig{worker.host, worker.grpc, worker.capacity, worker.downstreamNodes}; })
                        | std::ranges::to<std::vector>(),
                    local.singleNodeWorkerConfig);
            },
            [](const SystestRunner::DistributedExecution& distributed) -> UniquePtr<QuerySubmissionBackend>
            {
                const auto topologyPath = distributed.topologyPath.value_or(TEST_CONFIGURATION_DIR "/topologies/default_distributed.yaml");
                auto [nodes] = CLI::YamlLoader<CLI::ClusterConfig>::load(topologyPath);
                return std::make_unique<GRPCQuerySubmissionBackend>(
                    std::views::all(nodes)
                    | std::views::transform(
                        [](const CLI::WorkerConfig& worker) -> WorkerConfig
                        { return WorkerConfig{worker.host, worker.grpc, worker.capacity, worker.downstreamNodes}; })
                    | std::ranges::to<std::vector>());
            }},
        executionMode);
}
}

SystestRunner::SystestRunner(std::vector<PlannedQuery> inputQueries, ExecutionMode executionMode, const uint64_t queryConcurrency)
    : queryTracker{std::move(inputQueries), queryConcurrency}
    , executionMode{executionMode}
    , queryManager(createSubmissionBackend(executionMode))
    , reporter{std::make_unique<QueryResultReporter>(queryTracker.getTotalQueries())}
{
}

SystestRunner SystestRunner::from(std::vector<PlannedQuery> queries, ExecutionMode executionMode, const uint64_t queryConcurrency)
{
    return SystestRunner{std::move(queries), std::move(executionMode), queryConcurrency};
}

std::vector<FailedQuery> SystestRunner::run(nlohmann::json* _Nullable benchmarkResults) &&
{
    while (not queryTracker.done())
    {
        submit();
        handle();
    }

    if (benchmarkResults)
    {
        serializeBenchmarkResults(finishedQueries, *benchmarkResults);
    }

    return reportedFailures;
}

/// Submit as many queries as possible to the execution backend until either the max concurrency level is reached
/// or there are no more queries to submit.
/// Queries that have failed during planning are marked as failed and ignored.
/// Same thing applies to queries that fail during deployment.
void SystestRunner::submit()
{
    while (auto planned = queryTracker.nextPending())
    {
        auto planningResult = planned->planInfoOrException;
        planningResult
            .and_then(
                [this, &planned](auto& planInfo) mutable
                {
                    return queryManager.registerQuery(std::move(planInfo.plan))
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
                [this](auto&& submitted) -> std::expected<void, Exception>
                {
                    return queryManager.start(submitted.query)
                        .transform([this, &submitted] { queryTracker.moveToSubmitted(std::forward<decltype(submitted)>(submitted)); });
                })
            .or_else(
                [this, &planned](const Exception& exception)
                {
                    onFailure<PlannedQuery>(std::move(*planned), {exception});
                    return std::expected<void, Exception>{};
                });
    }
}

/// Consume from submitted queries and request their status from the execution backend.
/// If failed/stopped, report/check result, otherwise put back into running and continue
void SystestRunner::handle()
{
    while (auto submittedQuery = queryTracker.nextSubmitted())
    {
        queryManager
            .status(submittedQuery->query) /// NOLINT(*-unused-return-value)
            .and_then(
                [this, &submittedQuery](auto&& status) -> std::expected<void, std::vector<Exception>>
                {
                    switch (status.getGlobalQueryState())
                    {
                        case QueryState::Failed: {
                            /// Best effort error handling by terminating other local queries
                            if (const auto stopResult = queryManager.stop(submittedQuery->query); !stopResult)
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

    if (submitted.bytesProcessed.has_value() and submitted.tuplesProcessed.has_value())
    {
        throughput = calculateThroughput(*submitted.bytesProcessed, *submitted.tuplesProcessed, elapsedTime);
    }

    auto finished = FinishedQuery{std::move(submitted)};
    finished.elapsedTime = elapsedTime;
    finished.throughput = throughput;
    finishedQueries.push_back(finished);

    /// Report with performance information
    if (throughput)
    {
        const auto performanceMessage = fmt::format(" in {:.3f}s ({})", elapsedTime.count(), *throughput);
        reporter->reportSuccess(finished.ctx, performanceMessage);
    }
    const auto performanceMessage = fmt::format(" in {:.3f}s", elapsedTime.count());
    reporter->reportSuccess(finished.ctx, performanceMessage);

    queryTracker.moveToFinished(std::move(finished));
}
}

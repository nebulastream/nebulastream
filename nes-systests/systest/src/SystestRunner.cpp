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
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

#include <bits/chrono.h>
#include <fmt/format.h>

#include <Runtime/Execution/QueryStatus.hpp>
#include <Util/Overloaded.hpp>
#include <BenchmarkUtils.hpp>
#include <ErrorHandling.hpp>
#include <SystestResultCheck.hpp>
#include <SystestRunnerUtil.hpp>
#include <SystestState.hpp>
#include "QueryResultReporter.hpp"

namespace NES::Systest
{

using namespace std::chrono_literals;

SystestRunner::SystestRunner(std::vector<PlannedQuery> inputQueries, ExecutionMode executionMode, const uint64_t queryConcurrency)
    : queryTracker{inputQueries, queryConcurrency}
    , executionMode{executionMode}
    , reporter{std::make_unique<QueryResultReporter>(queryTracker.getTotalQueries())}
{
    std::visit(
        Overloaded{
            [this](const LocalExecution& local)
            { this->executionBackend = std::make_unique<EmbeddedWorkerBackend>(local.singleNodeWorkerConfig); },
            [this](const DistributedExecution& distributed)
            { this->executionBackend = std::make_unique<ClusterBackend>(distributed.topologyPath); },
            [this](const BenchmarkExecution& benchmark)
            { this->executionBackend = std::make_unique<EmbeddedWorkerBackend>(benchmark.singleNodeWorkerConfig); }},
        executionMode);
}

SystestRunner SystestRunner::from(std::vector<PlannedQuery> queries, ExecutionMode executionMode, const uint64_t queryConcurrency)
{
    return SystestRunner{std::move(queries), std::move(executionMode), queryConcurrency};
}

std::vector<FailedQuery> SystestRunner::run() &&
{
    while (not queryTracker.done())
    {
        submit();
        handle();
    }

    /// Serialize benchmark results if in benchmark mode
    if (std::holds_alternative<BenchmarkExecution>(executionMode))
    {
        const auto& [_, benchmarkResults] = std::get<BenchmarkExecution>(executionMode);
        serializeBenchmarkResults(finishedQueries, benchmarkResults);
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
        if (planned->planInfoOrException.has_value())
        {
            const auto& planInfo = planned->planInfoOrException.value();

            if (auto registerResult = executionBackend->registerQuery(planInfo.plan); registerResult.has_value())
            {
                auto submitted = SubmittedQuery{std::move(registerResult.value()), std::move(*planned)};

                /// Calculate source metrics for benchmark mode
                if (std::holds_alternative<BenchmarkExecution>(executionMode))
                {
                    auto [bytesProcessed, tuplesProcessed] = calculateSourceMetrics(submitted.planInfo);
                    submitted.bytesProcessed = bytesProcessed;
                    submitted.tuplesProcessed = tuplesProcessed;
                }

                executionBackend->startQuery(submitted.queryId);
                queryTracker.moveToSubmitted(std::move(submitted));
            }
            else
            {
                /// Error happened during query registration on at least one of the workers
                onFailure<PlannedQuery>(std::move(*planned), std::move(registerResult.error()));
            }
        }
        else
        {
            /// Error happened during query planning
            auto error = planned->planInfoOrException.error();
            onFailure<PlannedQuery>(std::move(*planned), std::vector{std::move(error)});
        }
    }
}

/// Consume from submitted queries and request their status from the execution backend.
/// If failed/stopped, report/check result, otherwise put back into running and continue
void SystestRunner::handle()
{
    while (auto submittedQuery = queryTracker.nextSubmitted())
    {
        switch (const auto queryStatus = executionBackend->status(submittedQuery->queryId); getGlobalQueryState(queryStatus))
        {
            case QueryState::Failed:
                /// Error happened during query execution, fetch exceptions from workers and handle
                onFailure<SubmittedQuery>(std::move(*submittedQuery), getExceptions(queryStatus));
                break;
            case QueryState::Stopped:
                onStopped(std::move(*submittedQuery));
                break;
            default: {
                /// onRunning, put it back
                queryTracker.moveToSubmitted(std::move(*submittedQuery));
                std::this_thread::sleep_for(25ms); /// Give other submitted queries a chance to make progress
                break;
            }
        }
    }
}

void SystestRunner::onStopped(SubmittedQuery&& submitted)
{
    /// Check if we expected an error but got success
    if (std::holds_alternative<ExpectedError>(submitted.ctx.expectedResultsOrError))
    {
        const auto expectedError = std::get<ExpectedError>(submitted.ctx.expectedResultsOrError);
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

    /// Handle benchmark mode logic
    if (std::holds_alternative<BenchmarkExecution>(executionMode))
    {
        const auto queryStatus = executionBackend->status(submitted.queryId);
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
        if (elapsedTime.has_value())
        {
            const auto performanceMessage
                = fmt::format(" in {:.3f}s ({})", elapsedTime->count(), throughput.value_or("no throughput data"));
            reporter->reportSuccess(finished.ctx, performanceMessage);
        }
        else
        {
            reporter->reportSuccess(finished.ctx, " (no timing data)");
        }

        queryTracker.moveToFinished(std::move(finished));
    }
    else
    {
        auto finished = FinishedQuery{std::move(submitted)};
        reporter->reportSuccess(finished.ctx);
        queryTracker.moveToFinished(std::move(finished));
    }
}

}

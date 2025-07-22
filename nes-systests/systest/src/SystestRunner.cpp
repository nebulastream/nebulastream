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

#include <thread>
#include <utility>
#include <variant>

#include <fmt/format.h>

#include <Util/Overloaded.hpp>
#include <BenchmarkUtils.hpp>
#include <ErrorHandling.hpp>
#include <SystestResultCheck.hpp>
#include <SystestRunnerUtil.hpp>

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
    finalizeBenchmarkResults();

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
        auto onFailure = [this, &planned](std::vector<Exception>&& exceptions)
        {
            INVARIANT(not exceptions.empty(), "Failed planning/register did not store any exceptions");
            auto failed = FailedQuery{std::move(*planned), std::move(exceptions)};
            reporter->reportFailure(failed.ctx, fmt::format("{}", failed.exceptions));
            reportedFailures.push_back(failed);
            queryTracker.markAsFailed(std::move(failed));
        };

        if (planned->planInfoOrException.has_value())
        {
            const auto& planInfo = planned->planInfoOrException.value();

            if (auto registerResult = executionBackend->registerQuery(planInfo.plan); registerResult.has_value())
            {
                auto submitted = SubmittedQuery{std::move(registerResult.value()), std::move(*planned)};

                /// Calculate source metrics for benchmark mode
                if (isBenchmarkMode())
                {
                    auto [bytesProcessed, tuplesProcessed] = calculateSourceMetrics(submitted.planInfo);
                    submitted.bytesProcessed = bytesProcessed;
                    submitted.tuplesProcessed = tuplesProcessed;
                }

                executionBackend->startQuery(submitted.queryId);
                queryTracker.markAsSubmitted(std::move(submitted));
            }
            else
            {
                onFailure(std::move(registerResult.error()));
            }
        }
        else
        {
            onFailure(std::vector{planned->planInfoOrException.error()});
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
                onFailure(*submittedQuery, queryStatus);
                break;
            case QueryState::Stopped:
                onStopped(*submittedQuery);
                break;
            default: {
                /// Query still running, put it back
                queryTracker.markAsSubmitted(std::move(*submittedQuery));
                std::this_thread::sleep_for(25ms);
                break;
            }
        }
    }
}

void SystestRunner::onFailure(SubmittedQuery& submitted, const QueryStatus& status)
{
    std::vector<Exception> exceptions = getExceptions(status);
    /// Handle expected errors vs actual failures
    if (std::holds_alternative<ExpectedError>(submitted.ctx.expectedResultsOrError))
    {
        const auto expectedError = std::get<ExpectedError>(submitted.ctx.expectedResultsOrError);
        INVARIANT(not exceptions.empty(), "Query status indicated failure, but no exceptions are contained");
        if (reporter->reportExpectedError(submitted.ctx, expectedError, exceptions.front()))
        {
            queryTracker.markAsFinished(FinishedQuery{std::move(submitted)});
            return;
        }
    }

    auto failed = FailedQuery{std::move(submitted), std::move(exceptions)};

    reporter->reportFailure(
        failed.ctx, failed.exceptions.empty() ? "Query failed without error details" : fmt::format("{}", failed.exceptions.front()));
    reportedFailures.push_back(failed);
    queryTracker.markAsFailed(std::move(failed));
}

void SystestRunner::onStopped(SubmittedQuery& submitted)
{
    /// Check if we expected an error but got success
    if (std::holds_alternative<ExpectedError>(submitted.ctx.expectedResultsOrError))
    {
        if (const auto expectedError = std::get<ExpectedError>(submitted.ctx.expectedResultsOrError);
            not reporter->reportUnexpectedSuccess(submitted.ctx, expectedError))
        {
            auto failedQuery = FailedQuery{std::move(submitted), {}};
            reportedFailures.push_back(failedQuery);
            queryTracker.markAsFailed(std::move(failedQuery));
            return;
        }
    }
    else
    {
        /// Check query results
        if (const auto errorMessage = checkResult(submitted))
        {
            auto failedQuery = FailedQuery{std::move(submitted), {}};
            reporter->reportFailure(failedQuery.ctx, *errorMessage);
            reportedFailures.push_back(failedQuery);
            queryTracker.markAsFailed(std::move(failedQuery));
            return;
        }
    }

    /// Calculate performance metrics for benchmark mode before moving submitted
    std::optional<std::chrono::duration<double>> elapsedTime;
    std::optional<std::string> throughput;

    if (isBenchmarkMode())
    {
        const auto queryStatus = executionBackend->status(submitted.queryId);
        elapsedTime = extractElapsedTime(queryStatus);

        if (elapsedTime.has_value() and submitted.bytesProcessed.has_value() and submitted.tuplesProcessed.has_value())
        {
            throughput = calculateThroughput(*submitted.bytesProcessed, *submitted.tuplesProcessed, *elapsedTime);
        }
    }

    auto finished = FinishedQuery{std::move(submitted)};
    finished.elapsedTime = elapsedTime;
    finished.throughput = throughput;

    if (isBenchmarkMode())
    {
        finishedQueries.push_back(finished);
    }

    /// Report success with performance information for benchmark mode
    if (isBenchmarkMode() and finished.elapsedTime.has_value())
    {
        const auto performanceMessage
            = fmt::format(" in {} ({})", finished.elapsedTime->count(), finished.throughput.value_or("no throughput data"));
        reporter->reportSuccess(finished.ctx, performanceMessage);
    }
    else
    {
        reporter->reportSuccess(finished.ctx);
    }

    queryTracker.markAsFinished(std::move(finished));
}

bool SystestRunner::isBenchmarkMode() const
{
    return std::holds_alternative<BenchmarkExecution>(executionMode);
}

void SystestRunner::finalizeBenchmarkResults() const
{
    if (not isBenchmarkMode())
    {
        return;
    }

    const auto& [_, benchmarkResults] = std::get<BenchmarkExecution>(executionMode);
    serializeBenchmarkResults(finishedQueries, benchmarkResults);
}

}

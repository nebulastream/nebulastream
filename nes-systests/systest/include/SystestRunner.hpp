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

#pragma once

#include <cstdint>
#include <filesystem>
#include <optional>
#include <ranges>
#include <string>
#include <utility>
#include <variant>

#include <nlohmann/json.hpp>

#include <Listeners/QueryLog.hpp>
#include <QueryManager/QueryManager.hpp>
#include <ErrorHandling.hpp>
#include <QueryResultReporter.hpp>
#include <QueryTracker.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <SystestState.hpp>

namespace NES::Systest
{

class SystestRunner
{
public:
    struct LocalExecution
    {
        std::optional<std::filesystem::path> topologyPath;
        SingleNodeWorkerConfiguration singleNodeWorkerConfig;
    };

    struct DistributedExecution
    {
        std::optional<std::filesystem::path> topologyPath;
    };

    using ExecutionMode = std::variant<LocalExecution, DistributedExecution>;

private:
    QueryTracker queryTracker;
    ExecutionMode executionMode;
    QueryManager queryManager;
    std::unique_ptr<QueryResultReporter> reporter;
    std::vector<FailedQuery> reportedFailures;
    std::vector<FinishedQuery> finishedQueries;

    SystestRunner(std::vector<PlannedQuery> inputQueries, ExecutionMode executionMode, uint64_t queryConcurrency);

public:
    static SystestRunner from(std::vector<PlannedQuery> queries, ExecutionMode executionMode, uint64_t queryConcurrency);

    SystestRunner() = delete;
    ~SystestRunner() = default;
    SystestRunner(const SystestRunner&) = delete;
    SystestRunner(SystestRunner&&) = delete;
    SystestRunner operator=(const SystestRunner&) = delete;
    SystestRunner operator=(SystestRunner&&) = delete;

    std::vector<FailedQuery> run(nlohmann::json* _Nullable benchmarkResults) &&;

private:
    void submit();
    void handle();
    void onStopped(SubmittedQuery&& submitted, const DistributedQueryStatus& queryStatus);

    template <typename QueryT>
    requires requires(QueryT qry) { qry.ctx; }
    void onFailure(QueryT&& query, std::vector<Exception>&& exceptions)
    {
        /// Check if test is negative test
        if (std::holds_alternative<ExpectedError>(query.ctx.expectedResultsOrError))
        {
            const auto& [code, message] = std::get<ExpectedError>(query.ctx.expectedResultsOrError);
            INVARIANT(not exceptions.empty(), "Query status indicated failure, but no exceptions were stored");

            /// Check if the actual error matches the expected error
            if (code == exceptions.front().code())
            {
                reporter->reportSuccess(query.ctx);
                queryTracker.moveToFinished(FinishedQuery{std::forward<QueryT>(query)});
                return;
            }

            const auto errorMessage = fmt::format("expected error {} but got {}", code, exceptions.front().code());
            auto failed = FailedQuery{std::forward<QueryT>(query).ctx, std::move(exceptions)};
            reporter->reportFailure(failed.ctx, std::move(errorMessage));
            reportedFailures.push_back(failed);
            queryTracker.moveToFailed(std::move(failed));
            return;
        }

        auto failed = FailedQuery{std::forward<QueryT>(query).ctx, std::move(exceptions)};
        auto errorMessage = failed.exceptions.empty() ? "Query failed without error details"
                                                      : fmt::format(
                                                            "Query failed with {} exception(s):\n{}",
                                                            failed.exceptions.size(),
                                                            fmt::join(
                                                                failed.exceptions | std::views::enumerate
                                                                    | std::views::transform(
                                                                        [](const auto& pair)
                                                                        {
                                                                            const auto& [index, exception] = pair;
                                                                            return fmt::format("  [{}] {}", index + 1, exception);
                                                                        }),
                                                                "\n"));
        reporter->reportFailure(failed.ctx, std::move(errorMessage));
        reportedFailures.push_back(failed);
        queryTracker.moveToFailed(std::move(failed));
    }
};

}

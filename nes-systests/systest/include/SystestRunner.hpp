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
#include <memory>
#include <variant>
#include <vector>

#include <nlohmann/json_fwd.hpp>

#include <BenchmarkUtils.hpp>
#include <ExecutionBackend.hpp>
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
        SingleNodeWorkerConfiguration singleNodeWorkerConfig;
    };
    struct DistributedExecution
    {
        std::filesystem::path topologyPath;
    };
    struct BenchmarkExecution
    {
        SingleNodeWorkerConfiguration singleNodeWorkerConfig;
        /// Type is bound to the variant, which in turn is bound to the outer object, which is single-use
        /// Therefore, storing the ref here is safe
        nlohmann::json& benchmarkResults; /// NOLINT(*-avoid-const-or-ref-data-members)
    };

    using ExecutionMode = std::variant<LocalExecution, DistributedExecution, BenchmarkExecution>;

private:
    QueryTracker queryTracker;
    ExecutionMode executionMode;
    std::unique_ptr<ExecutionBackend> executionBackend;
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

    std::vector<FailedQuery> run() &&;

private:
    void submit();
    void handle();
    void onFailure(SubmittedQuery& submitted, const QueryStatus& status);
    void onStopped(SubmittedQuery& submitted);
    
    [[nodiscard]] bool isBenchmarkMode() const;
    void finalizeBenchmarkResults() const;
};

}

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

#include <cstddef>
#include <cstdint>
#include <expected>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
#include <DataTypes/Schema.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <nlohmann/json_fwd.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <SystestState.hpp>

namespace NES::Systest
{
/// Forward declarations
struct SystestQuery;
struct RunningQuery;
class QuerySubmitter;

/// Pad size of (PASSED / FAILED) in the console output of the systest to have a nicely looking output
static constexpr auto padSizeSuccess = 120;
/// We pad to a maximum of 3 digits ---> maximum value that is correctly padded is 99 queries per file
static constexpr auto padSizeQueryNumber = 2;
/// We pad to a maximum of 4 digits ---> maximum value that is correctly padded is 999 queries in total
static constexpr auto padSizeQueryCounter = 3;

/// Runs queries
/// @return returns a collection of failed queries
using SuccessCallback = std::function<std::string(RunningQuery&)>;
[[nodiscard]] std::vector<RunningQuery> runQueries(
    const std::vector<SystestQuery>& queries,
    uint64_t numConcurrentQueries,
    QuerySubmitter& querySubmitter,
    SuccessCallback&& successCallback = [](auto) { return ""; });

/// Run queries locally ie not on single-node-worker in a separate process
/// @return returns a collection of failed queries
[[nodiscard]] std::vector<RunningQuery> runQueriesAtLocalWorker(
    const std::vector<SystestQuery>& queries, uint64_t numConcurrentQueries, const SingleNodeWorkerConfiguration& configuration);

/// Run queries remote on the single-node-worker specified by the URI
/// @return returns a collection of failed queries
[[nodiscard]] std::vector<RunningQuery>
runQueriesAtRemoteWorker(const std::vector<SystestQuery>& queries, uint64_t numConcurrentQueries, const std::string& serverURI);

/// Run queries sequentially locally and benchmark the run time of each query.
/// @return vector containing failed queries
[[nodiscard]] std::vector<RunningQuery> runQueriesAndBenchmark(
    const std::vector<SystestQuery>& queries, const SingleNodeWorkerConfiguration& configuration, nlohmann::json& resultJson);

/// Prints the error message, if the query has failed/passed and the expected and result tuples, like below
/// function/arithmetical/FunctionDiv:4..................................Passed
/// function/arithmetical/FunctionMul:5..................................Failed
/// SELECT * FROM s....
/// Expected ............ | Actual 1, 2,3
void printQueryResultToStdOut(
    const RunningQuery& runningQuery,
    const std::string& errorMessage,
    size_t queryCounter,
    size_t totalQueries,
    const std::string_view queryPerformanceMessage);

}

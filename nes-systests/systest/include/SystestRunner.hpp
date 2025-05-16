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
#include <filesystem>
#include <memory>
#include <string>
#include <string_view>
#include <vector>
#include <Plans/LogicalPlan.hpp>
#include <nlohmann/json_fwd.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <SystestParser.hpp>

namespace NES::Systest
{
/// Forward refs to avoid cyclic deps with SystestState
struct Query;
struct RunningQuery;

struct LoadedQueryPlan
{
    LogicalPlan queryPlan;
    std::string queryName;
    SystestParser::Schema sinkSchema;
    std::unordered_map<std::string, std::pair<std::filesystem::path, uint64_t>> sourceNamesToFilepathAndCount;
};

/// Pad size of (PASSED / FAILED) in the console output of the systest to have a nicely looking output
static constexpr auto padSizeSuccess = 120;
/// We pad to a maximum of 3 digits ---> maximum value that is correctly padded is 99 queries per file
static constexpr auto padSizeQueryNumber = 2;
/// We pad to a maximum of 4 digits ---> maximum value that is correctly padded is 999 queries in total
static constexpr auto padSizeQueryCounter = 3;

/// Load query plan objects by parsing an SLT file for queries and lowering it
/// Returns a triplet of the lowered query plan, the query name and the schema of the sink
[[nodiscard]] std::vector<LoadedQueryPlan> loadFromSLTFile(
    const std::filesystem::path& testFilePath,
    const std::filesystem::path& workingDir,
    std::string_view testFileName,
    const std::filesystem::path& testDataDir);

/// Run queries locally ie not on single-node-worker in a separate process
/// @return false if one query result is incorrect
[[nodiscard]] std::vector<RunningQuery> runQueriesAtLocalWorker(
    const std::vector<Query>& queries, uint64_t numConcurrentQueries, const Configuration::SingleNodeWorkerConfiguration& configuration);

/// Run queries remote on the single-node-worker specified by the URI
/// @return false if one query result is incorrect
[[nodiscard]] std::vector<RunningQuery>
runQueriesAtRemoteWorker(const std::vector<Query>& queries, uint64_t numConcurrentQueries, const std::string& serverURI);


/// Run queries sequentially locally and benchmark the run time of each query.
/// @return vector containing failed queries
[[nodiscard]] std::vector<RunningQuery> runQueriesAndBenchmark(
    const std::vector<Query>& queries, const Configuration::SingleNodeWorkerConfiguration& configuration, nlohmann::json& resultJson);

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

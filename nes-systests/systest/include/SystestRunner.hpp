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
#include <string>
#include <utility>
#include <vector>
#include <Operators/Serialization/DecomposedQueryPlanSerializationUtil.hpp>
#include <SingleNodeWorkerConfiguration.hpp>

namespace NES::Systest
{
/// Forward refs to avoid cyclic deps with SystestState
struct Query;
struct RunningQuery;

/// Pad size of (PASSED / FAILED) in the console output of the systest to have a nicely looking output
static constexpr auto padSizeSuccess = 60;
/// We pad to a maximum of 3 digits ---> maximum value that is correctly padded is 99 queries
static constexpr auto padSizeQueryNumber = 2;

/// Load query plan objects by parsing an SLT file for queries and lowering it
[[nodiscard]] std::vector<std::pair<DecomposedQueryPlanPtr, std::string>>
loadFromSLTFile(const std::filesystem::path& testFilePath, const std::filesystem::path& resultDir, const std::string& testFileName);

/// Run queries locally ie not on single-node-worker in a separate process
/// @return false if one query result is incorrect
[[nodiscard]] std::vector<RunningQuery> runQueriesAtLocalWorker(
    const std::vector<Query>& queries, uint64_t numConcurrentQueries, const Configuration::SingleNodeWorkerConfiguration& configuration);

/// Run queries remote on the single-node-worker specified by the URI
/// @return false if one query result is incorrect
[[nodiscard]] std::vector<RunningQuery>
runQueriesAtRemoteWorker(const std::vector<Query>& queries, uint64_t numConcurrentQueries, const std::string& serverURI);

/// Prints the error message, if the query has failed/passed and the expected and result tuples, like below
/// function/arithmetical/FunctionDiv:4..................................Passed
/// function/arithmetical/FunctionMul:5..................................Failed
/// SELECT * FROM s....
/// Expected ............ | Actual 1, 2,3
void printQueryResultToStdOut(const Query& query, const std::string& errorMessage);

}

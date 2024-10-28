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

#include <filesystem>
#include <string>
#include <Operators/Serialization/DecomposedQueryPlanSerializationUtil.hpp>
#include <SingleNodeWorkerConfiguration.hpp>

namespace NES::Systest
{

/// Forward refs to avoid cyclic deps with SystestState
struct Query;

/// Load query plan objects by parsing a SLT file for queries and lowering it
std::vector<DecomposedQueryPlanPtr>
loadFromSLTFile(const std::filesystem::path& testFilePath, const std::filesystem::path& resultDir, const std::string& testname);

/// Run queries locally ie not on single-node-worker in a separate process
/// @return false if one query result is incorrect
bool runQueriesAtLocalWorker(
    std::vector<NES::Systest::Query> queries,
    uint64_t numConcurrentQueries,
    const NES::Configuration::SingleNodeWorkerConfiguration& configuration);

/// Run queries remote on the single-node-worker specified by the URI
/// @return false if one query result is incorrect
bool runQueriesAtRemoteWorker(std::vector<NES::Systest::Query> queries, uint64_t numConcurrentQueries, const std::string& serverURI);

}

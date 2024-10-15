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

#include <SystestConfiguration.hpp>
#include <SerializableDecomposedQueryPlan.pb.h>

namespace NES::Systest
{

using DecomposedQueryPlanPtr = std::shared_ptr<class DecomposedQueryPlan>;
struct TestFile;
struct TestGroup;


// collect query objects --> before running
std::vector<Query> collectQueriesToTest(const Configuration::SystestConfiguration& configuration);

// Laad decomposed query plans
std::vector<DecomposedQueryPlanPtr>
loadFromSLTFile(const std::filesystem::path& testFilePath, const std::filesystem::path& resultDir, const std::string& testname);
std::vector<SerializableDecomposedQueryPlan> loadFromCacheFiles(const std::vector<std::filesystem::path>& cacheFiles);

// check the query result
bool checkResult(const Query& query);
}

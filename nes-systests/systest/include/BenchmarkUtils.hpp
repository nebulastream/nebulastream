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

#include <chrono>
#include <cstddef>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <nlohmann/json_fwd.hpp>

#include <SystestState.hpp>

namespace NES::Systest
{

/// Benchmark performance metrics for a query
struct BenchmarkMetrics
{
    size_t bytesProcessed = 0;
    size_t tuplesProcessed = 0;
    std::chrono::duration<double> elapsedTime{};
    std::string throughputDescription;
};

/// Calculate source file metrics (bytes and tuples processed) from PlanInfo
std::pair<size_t, size_t> calculateSourceMetrics(const PlanInfo& planInfo);

/// Extract elapsed time from QueryStatus (using the last run's timing)
std::optional<std::chrono::duration<double>> extractElapsedTime(const QueryStatus& status);

/// Calculate and format throughput string from metrics
std::string calculateThroughput(size_t bytesProcessed, size_t tuplesProcessed, std::chrono::duration<double> elapsedTime);

/// Create BenchmarkMetrics from PlanInfo and QueryStatus
BenchmarkMetrics createBenchmarkMetrics(const PlanInfo& planInfo, const QueryStatus& status);

/// Serialize finished queries with benchmark metrics to JSON
void serializeBenchmarkResults(const std::vector<FinishedQuery>& finished, nlohmann::json& resultJson);

}

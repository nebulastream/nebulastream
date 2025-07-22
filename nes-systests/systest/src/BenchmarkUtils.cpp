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

#include <BenchmarkUtils.hpp>

#include <algorithm>
#include <array>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <optional>
#include <ranges>
#include <string>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <nlohmann/json.hpp>

#include <ErrorHandling.hpp>
#include <SystestState.hpp>

namespace NES::Systest
{

std::pair<size_t, size_t> calculateSourceMetrics(const PlanInfo& planInfo)
{
    size_t totalBytesProcessed = 0;
    size_t totalTuplesProcessed = 0;

    for (const auto& sourceFileAndCount : planInfo.sourcesToFilePathsAndCounts | std::views::values)
    {
        const auto& [sourceFile, occurrenceCount] = sourceFileAndCount;
        const auto& filePath = sourceFile.getRawValue();

        if (not(std::filesystem::exists(filePath) and filePath.has_filename()))
        {
            NES_ERROR("Source path is empty or does not exist: {}", filePath.string());
            return {0, 0};
        }

        /// Calculate bytes processed
        const size_t fileSize = std::filesystem::file_size(filePath);
        totalBytesProcessed += fileSize * occurrenceCount;

        /// Calculate tuples processed by counting newlines
        std::ifstream inFile(filePath);
        const size_t lineCount = std::count(std::istreambuf_iterator(inFile), std::istreambuf_iterator<char>(), '\n');
        totalTuplesProcessed += lineCount * occurrenceCount;
    }

    return {totalBytesProcessed, totalTuplesProcessed};
}

std::optional<std::chrono::duration<double>> extractElapsedTime(const QueryStatus& status)
{
    /// Find the first local status with timing information
    for (const auto& localStatus : status.localStatusSnapshots)
    {
        if (localStatus.metrics.running.has_value() and localStatus.metrics.stop.has_value())
        {
            return std::chrono::duration_cast<std::chrono::duration<double>>(
                localStatus.metrics.stop.value() - localStatus.metrics.running.value());
        }
    }
    return std::nullopt;
}

std::string calculateThroughput(const size_t bytesProcessed, const size_t tuplesProcessed, const std::chrono::duration<double> elapsedTime)
{
    if (bytesProcessed == 0 or tuplesProcessed == 0 or elapsedTime.count() <= 0.0)
    {
        return "";
    }

    const double bytesPerSecond = static_cast<double>(bytesProcessed) / elapsedTime.count();
    const double tuplesPerSecond = static_cast<double>(tuplesProcessed) / elapsedTime.count();

    auto formatUnits = [](double throughput)
    {
        /// Format throughput in SI units, e.g. 1.234 MB/s instead of 1234000 B/s
        const std::array<std::string, 5> units = {"", "k", "M", "G", "T"};
        size_t unitIndex = 0;
        constexpr auto nextUnit = 1000;
        while (throughput >= nextUnit && unitIndex < units.size() - 1)
        {
            throughput /= nextUnit;
            unitIndex++;
        }
        return fmt::format("{:.3f} {}", throughput, units[unitIndex]);
    };

    return fmt::format("{}B/s / {}Tup/s", formatUnits(bytesPerSecond), formatUnits(tuplesPerSecond));
}

BenchmarkMetrics createBenchmarkMetrics(const PlanInfo& planInfo, const QueryStatus& status)
{
    BenchmarkMetrics metrics;

    /// Calculate source metrics
    auto [bytesProcessed, tuplesProcessed] = calculateSourceMetrics(planInfo);
    metrics.bytesProcessed = bytesProcessed;
    metrics.tuplesProcessed = tuplesProcessed;

    /// Extract elapsed time
    if (const auto elapsedTime = extractElapsedTime(status))
    {
        metrics.elapsedTime = *elapsedTime;
        metrics.throughputDescription = calculateThroughput(bytesProcessed, tuplesProcessed, *elapsedTime);
    }

    return metrics;
}

void serializeBenchmarkResults(const std::vector<FinishedQuery>& finished, nlohmann::json& resultJson)
{
    nlohmann::json queryResults = nlohmann::json::array();

    for (const auto& finishedQuery : finished)
    {
        nlohmann::json queryResult;
        queryResult["testName"] = finishedQuery.ctx.testName;
        queryResult["passed"] = true; /// If it's in finishedQueries, it passed

        /// Add performance metrics if available
        if (finishedQuery.elapsedTime.has_value())
        {
            queryResult["elapsedTimeSeconds"] = finishedQuery.elapsedTime->count();
        }

        if (finishedQuery.throughput.has_value())
        {
            queryResult["throughput"] = *finishedQuery.throughput;
        }

        /// Add source metrics if we have them from the original calculation
        /// Note: These were calculated during submission and stored in the SubmittedQuery
        /// but are not directly accessible in FinishedQuery. We'll calculate them again from PlanInfo.
        if (finishedQuery.planInfo.has_value())
        {
            if (auto [bytesProcessed, tuplesProcessed] = calculateSourceMetrics(*finishedQuery.planInfo);
                bytesProcessed > 0 or tuplesProcessed > 0)
            {
                queryResult["bytesProcessed"] = bytesProcessed;
                queryResult["tuplesProcessed"] = tuplesProcessed;
            }
        }

        queryResults.push_back(queryResult);
    }

    resultJson["queryResults"] = queryResults;
    resultJson["totalQueries"] = finished.size();
    resultJson["passedQueries"] = finished.size();
    resultJson["failedQueries"] = 0; /// This function only handles successful queries
}

}
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

#include <Benchmark.hpp>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <ranges>
#include <span>
#include <string>
#include <system_error>
#include <utility>
#include <variant>
#include <vector>

#include <fmt/base.h>
#include <fmt/format.h>
#include <rfl/json/write.hpp>

#include <Model/Expectation.hpp>
#include <Model/RunnableTestFile.hpp>
#include <Model/TestCaseId.hpp>
#include <Model/Verdict.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{

/// A self-join counts its input file twice.
/// An unreadable file adds nothing: under-reporting beats aborting the measurement.
std::pair<uint64_t, uint64_t> measureInputSize(const std::vector<std::filesystem::path>& inputFiles)
{
    uint64_t bytes = 0;
    uint64_t rows = 0;
    for (const auto& file : inputFiles)
    {
        std::error_code errorCode;
        if (const auto size = std::filesystem::file_size(file, errorCode); not errorCode)
        {
            bytes += size;
            std::ifstream contents{file};
            rows += static_cast<uint64_t>(std::count(std::istreambuf_iterator<char>{contents}, std::istreambuf_iterator<char>{}, '\n'));
        }
    }
    return {bytes, rows};
}

double computeRate(const uint64_t amount, const std::chrono::milliseconds took)
{
    return took.count() > 0 ? static_cast<double>(amount) / std::chrono::duration<double>{took}.count() : 0.0;
}

}

/// A differential block has no input files, an EXPLAIN never runs, and a negative test would measure the time to its failure.
void Benchmark::record(const ReportEntry& entry, const RewrittenTestCase& testCase, const std::span<const StatementTiming> timings)
{
    if (const auto* query = std::get_if<RewrittenQuery>(&testCase.action); query != nullptr and hasPassed(entry.outcome)
        and not timings.empty() and not std::holds_alternative<ExpectedError>(query->expected))
    {
        record(fmt::format("{}", entry.id), query->inputFiles, timings.front().execution);
    }
}

void Benchmark::record(
    const std::string& name, const std::vector<std::filesystem::path>& inputFiles, const std::chrono::milliseconds execution)
{
    if (execution.count() <= 0)
    {
        return;
    }

    if (const auto known = std::ranges::find(measurements, name, &Measurement::name); known != measurements.end())
    {
        /// Best, not mean: a slower round measured contention, not the query.
        known->best = std::min(known->best, execution);
        return;
    }

    const auto [bytes, rows] = measureInputSize(inputFiles);
    measurements.push_back(Measurement{.name = name, .best = execution, .bytes = bytes, .tuples = rows});
}

std::vector<BenchmarkRow> Benchmark::buildRows() const
{
    return measurements
        | std::views::transform(
               [](const Measurement& measurement)
               {
                   return BenchmarkRow{
                       .queryName = measurement.name,
                       .time = std::chrono::duration_cast<std::chrono::duration<double>>(measurement.best).count(),
                       .bytesPerSecond = computeRate(measurement.bytes, measurement.best),
                       .tuplesPerSecond = computeRate(measurement.tuples, measurement.best)};
               })
        | std::ranges::to<std::vector<BenchmarkRow>>();
}

std::string Benchmark::writeTo(const std::filesystem::path& report) const
{
    const auto table = buildRows();
    for (const auto& row : table)
    {
        fmt::print(
            "{:<60} {:>9.3f} s  {:>12.0f} tuples/s  {:>14.0f} bytes/s\n",
            row.queryName.value(),
            row.time,
            row.tuplesPerSecond,
            row.bytesPerSecond);
    }

    std::filesystem::create_directories(report.parent_path());
    std::ofstream out{report};
    if (not out)
    {
        throw TestException("could not open the benchmark report {}", report.string());
    }
    out << rfl::json::write(table);
    if (not out)
    {
        throw TestException("could not write the benchmark report {}", report.string());
    }
    return fmt::format("{} queries measured, written to {}\n", table.size(), report.string());
}

}

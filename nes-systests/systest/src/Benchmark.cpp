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
#include <string>
#include <system_error>
#include <vector>

#include <fmt/format.h>
#include <rfl/json/write.hpp>

#include <ErrorHandling.hpp>

namespace NES
{
namespace
{

/// The bytes and rows a query read, summed over its input files.
/// A file counts once per reference, so a query joining a source with itself reads it twice.
/// A file the run cannot open contributes nothing, which under-reports rather than aborting a measurement.
std::pair<uint64_t, uint64_t> inputSize(const std::vector<std::filesystem::path>& inputFiles)
{
    uint64_t bytes = 0;
    uint64_t rows = 0;
    for (const auto& file : inputFiles)
    {
        std::error_code errorCode;
        const auto size = std::filesystem::file_size(file, errorCode);
        if (errorCode)
        {
            continue;
        }
        bytes += size;
        std::ifstream contents{file};
        rows += static_cast<uint64_t>(std::count(std::istreambuf_iterator<char>{contents}, std::istreambuf_iterator<char>{}, '\n'));
    }
    return {bytes, rows};
}

/// How many of something happened per second, and zero when nothing was measured.
double perSecond(const uint64_t amount, const std::chrono::milliseconds took)
{
    if (took.count() <= 0)
    {
        return 0.0;
    }
    return static_cast<double>(amount) * 1000.0 / static_cast<double>(took.count());
}

}

void Benchmark::record(
    const std::string& name, const std::vector<std::filesystem::path>& inputFiles, const std::chrono::milliseconds execution)
{
    if (execution.count() <= 0)
    {
        return;
    }

    if (const auto known = std::ranges::find(measured, name, &Measured::name); known != measured.end())
    {
        known->best = std::min(known->best, execution);
        return;
    }

    const auto [bytes, rows] = inputSize(inputFiles);
    measured.push_back(Measured{.name = name, .best = execution, .bytes = bytes, .tuples = rows});
}

std::vector<BenchmarkRow> Benchmark::rows() const
{
    return measured
        | std::views::transform(
               [](const Measured& query)
               {
                   return BenchmarkRow{
                       .queryName = query.name,
                       .time = std::chrono::duration_cast<std::chrono::duration<double>>(query.best).count(),
                       .bytesPerSecond = perSecond(query.bytes, query.best),
                       .tuplesPerSecond = perSecond(query.tuples, query.best)};
               })
        | std::ranges::to<std::vector<BenchmarkRow>>();
}

std::string Benchmark::writeTo(const std::filesystem::path& report) const
{
    const auto measurements = rows();
    for (const auto& row : measurements)
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
    out << rfl::json::write(measurements);
    if (not out)
    {
        throw TestException("could not write the benchmark report {}", report.string());
    }
    return fmt::format("{} queries measured, written to {}\n", measurements.size(), report.string());
}

}

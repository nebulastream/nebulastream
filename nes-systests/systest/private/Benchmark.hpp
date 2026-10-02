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
#include <cstdint>
#include <filesystem>
#include <span>
#include <string>
#include <vector>

#include <rfl/Rename.hpp>

#include <Model/RunnableTestFile.hpp>
#include <Model/Verdict.hpp>

namespace NES
{

struct BenchmarkRow
{
    rfl::Rename<"query name", std::string> queryName;
    double time = 0.0;
    double bytesPerSecond = 0.0;
    double tuplesPerSecond = 0.0;
};

/// Keeps the best time per query and writes the JSON report.
class Benchmark
{
public:
    /// Ignores anything but a plain query that ran and passed.
    void record(const ReportEntry& entry, const RewrittenTestCase& testCase, std::span<const StatementTiming> timings);

    void record(const std::string& name, const std::vector<std::filesystem::path>& inputFiles, std::chrono::nanoseconds execution);

    /// Returns the rows in the order the queries were first measured.
    [[nodiscard]] std::vector<BenchmarkRow> buildRows() const;

    /// Prints the rows, writes them as JSON, and returns the summary line.
    [[nodiscard]] std::string writeReport(const std::filesystem::path& reportPath) const;

private:
    struct Measurement
    {
        std::string name;
        std::chrono::nanoseconds best{};
        uint64_t bytes = 0;
        uint64_t tuples = 0;
    };

    std::vector<Measurement> measurements;
};

}

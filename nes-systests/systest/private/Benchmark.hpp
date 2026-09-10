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
#include <string>
#include <vector>

#include <rfl/Rename.hpp>

namespace NES
{

/// What one query measured, serialized to the report file.
/// The field names below are the JSON keys, and the first keeps the historical spelling with a space that readers of
/// this file already expect, which a C++ identifier cannot carry.
struct BenchmarkRow
{
    rfl::Rename<"query name", std::string> queryName;
    double time = 0.0;
    double bytesPerSecond = 0.0;
    double tuplesPerSecond = 0.0;
};

/// Collects the best measurement of each query across the rounds of a benchmark.
/// The best rather than the mean, because a slower round means the query competed with something, and the fastest run
/// is the one that measured the query rather than the machine.
class Benchmark
{
public:
    /// Records one query's time under the given report name, keeping it only when it beats what that query has done before.
    /// A query that reached no terminal state took no measurable time and is left out.
    void record(const std::string& name, const std::vector<std::filesystem::path>& inputFiles, std::chrono::milliseconds execution);

    /// Writes the rows as JSON and prints them, and returns what the invocation reports.
    [[nodiscard]] std::string writeTo(const std::filesystem::path& report) const;

private:
    /// The rows in the order the queries were first measured.
    [[nodiscard]] std::vector<BenchmarkRow> rows() const;

    /// One query's best time so far, and what it read.
    struct Measured
    {
        std::string name;
        std::chrono::milliseconds best{};
        uint64_t bytes = 0;
        uint64_t tuples = 0;
    };

    std::vector<Measured> measured;
};

}

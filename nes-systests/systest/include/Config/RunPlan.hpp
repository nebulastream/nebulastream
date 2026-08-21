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
#include <filesystem>
#include <optional>
#include <variant>

namespace NES
{

/// Runs the queries in file order.
struct SourceOrder
{
};

/// Runs the queries in a random order, so a test that only passes because another one ran before it is found.
struct Shuffled
{
};

using OrderingPolicy = std::variant<SourceOrder, Shuffled>;

/// Submits every query once, which is what a test run does.
struct Once
{
};

/// Submits the queries round after round, until something outside the run stops it.
struct UntilStopped
{
};

using RepetitionPolicy = std::variant<Once, UntilStopped>;

class SystestConfiguration;

/// One invocation's plan, read once from the command line options.
/// The config still parses the command line. The run follows the plan rather than single options, so options that only
/// mean something together are read in one place.
struct RunPlan
{
    OrderingPolicy ordering;
    size_t concurrency = 1;
    RepetitionPolicy repetition;
    /// Where a measuring run writes its report. Absent means the run measures nothing.
    /// A measuring run checks its results like any other run, because a fast wrong answer is not a measurement.
    std::optional<std::filesystem::path> measureReport;

    /// Reads the options into the plan that the run follows.
    [[nodiscard]] static RunPlan create(const SystestConfiguration& config);
};

}

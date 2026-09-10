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
#include <cstdint>
#include <filesystem>
#include <optional>
#include <variant>

namespace NES
{

/// Runs the test files in the order discovery found them.
struct SourceOrder
{
};

/// Runs the test files in a random order, so a test that only passes because another one ran before it is found.
/// An absent seed draws one, and the run prints it so the order that failed can be repeated.
struct Shuffled
{
    std::optional<uint64_t> seed;
};

using OrderingPolicy = std::variant<SourceOrder, Shuffled>;

/// Submits every query once, which is what an ordinary test run does.
struct Once
{
};

/// Submits every query this many rounds.
struct FixedRounds
{
    uint64_t count = 1;
};

/// Submits the queries round after round without a round limit, so only the run's time limit ends it.
struct UntilLimit
{
};

using RepetitionPolicy = std::variant<Once, FixedRounds, UntilLimit>;

class Config;

/// The plan one invocation runs, interpreted once from the command line options.
/// The config stays the parsing surface, and the run reads this instead of single options, so options that only mean
/// something together are combined in one place.
struct RunPlan
{
    OrderingPolicy ordering;
    size_t concurrency = 1;
    RepetitionPolicy repetition;
    /// How long a repeating run keeps starting new rounds. Absent keeps going until the rounds run out.
    std::optional<std::chrono::seconds> runLimit;
    /// Where a measuring run writes its report.
    /// Engaged turns the run into a measurement: each round records how long every passing query took, and the checks
    /// keep running underneath, because a fast wrong answer is not a measurement.
    std::optional<std::filesystem::path> measureReport;

    /// Interprets the command line options into the plan the run follows.
    [[nodiscard]] static RunPlan create(const Config& config);
};

}

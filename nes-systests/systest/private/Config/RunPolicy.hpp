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

/// Runs the queries in file order (default).
struct RunInFileOrder
{
};

/// Runs the queries in a random order, which is how tests that depend on each other are found.
/// Every query in a run is submitted to the same worker, so a test can pass on state that an earlier query left behind.
/// An absent seed draws one, and the run prints it so the order that failed can be repeated.
struct RunInShuffledOrder
{
    std::optional<uint64_t> seed;
};

using OrderingPolicy = std::variant<RunInFileOrder, RunInShuffledOrder>;

/// Submits every query once (default).
struct SubmitOnce
{
};

/// Submits the queries this many rounds.
struct SubmitRounds
{
    uint64_t count = 1;
};

/// Submits the queries round after round without a round limit, so only the run's time limit or a failure ends it.
struct SubmitUntilStopped
{
};

using RepetitionPolicy = std::variant<SubmitOnce, SubmitRounds, SubmitUntilStopped>;

class SystestConfiguration;

/// One invocation's policy, read once from the systest config.
struct RunPolicy
{
    RunPolicy() = delete;

    /// Reads the options into the policy that the run follows.
    [[nodiscard]] static RunPolicy create(const SystestConfiguration& config);

    OrderingPolicy ordering;
    /// How many queries the run submits at once.
    size_t concurrency;
    RepetitionPolicy repetition;
    /// How long a repeating run keeps starting new rounds. Absent keeps going until the rounds run out.
    std::optional<std::chrono::seconds> runLimit;
    /// `std::nullopt` means the run measures nothing.
    /// Engaged turns the run into a measurement: each round records how long every passing query took, and the checks
    /// keep running underneath, because a fast wrong answer is not a measurement.
    std::optional<std::filesystem::path> measureReport;

private:
    RunPolicy(
        OrderingPolicy ordering,
        size_t concurrency,
        RepetitionPolicy repetition,
        std::optional<std::chrono::seconds> runLimit,
        std::optional<std::filesystem::path> measureReport);
};

}

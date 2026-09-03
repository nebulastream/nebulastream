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

/// Runs the queries in file order (default).
struct RunInFileOrder
{
};

/// Runs the queries in a random order, which is how tests that depend on each other are found.
/// Every query in a run is submitted to the same worker, so a test can pass on state that an earlier query left behind.
struct RunInShuffledOrder
{
};

using OrderingPolicy = std::variant<RunInFileOrder, RunInShuffledOrder>;

/// Submits every query once (default).
struct SubmitOnce
{
};

/// Submits the queries round after round, until something outside the run stops it.
struct SubmitUntilStopped
{
};

using RepetitionPolicy = std::variant<SubmitOnce, SubmitUntilStopped>;

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
    /// `std::nullopt` means the run measures nothing.
    std::optional<std::filesystem::path> measureReport;

private:
    RunPolicy(OrderingPolicy ordering, size_t concurrency, RepetitionPolicy repetition, std::optional<std::filesystem::path> measureReport);
};

}

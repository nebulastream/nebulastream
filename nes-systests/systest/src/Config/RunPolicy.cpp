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

#include <Config/RunPolicy.hpp>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <random>
#include <utility>

#include <Config/Config.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

static OrderingPolicy orderingFrom(const SystestConfiguration& config)
{
    if (not config.randomQueryOrder)
    {
        return RunInFileOrder{};
    }
    /// A zero seed means the user set none, so the run picks a random one.
    const auto seed = config.shuffleSeed;
    return RunInShuffledOrder{.seed = seed == 0 ? uint64_t{std::random_device{}()} : seed};
}

RunPolicy::RunPolicy(
    const OrderingPolicy ordering,
    const size_t concurrency,
    const RepetitionPolicy repetition,
    const std::optional<std::chrono::seconds> runLimit,
    std::optional<std::filesystem::path> measureReport)
    : ordering(ordering), concurrency(concurrency), repetition(repetition), runLimit(runLimit), measureReport(std::move(measureReport))
{
}

RunPolicy RunPolicy::create(const SystestConfiguration& config)
{
    const auto measuring = config.benchmark;
    if (measuring and config.endlessMode)
    {
        throw InvalidConfigParameter("a benchmark run cannot also run in endless mode");
    }
    /// A measuring run submits one query at a time, because queries that run together share the worker and their timings
    /// would depend on each other.
    const auto concurrency = measuring ? 1 : config.numberConcurrentQueries;

    if (measuring)
    {
        return RunPolicy{
            orderingFrom(config),
            concurrency,
            SubmitRounds{.count = std::max(uint64_t{1}, config.benchmarkRounds)},
            std::nullopt,
            std::filesystem::path{config.workingDir} / "BenchmarkResults.json"};
    }

    RepetitionPolicy repetition = SubmitOnce{};
    std::optional<std::chrono::seconds> runLimit;
    if (config.endlessMode)
    {
        const auto rounds = config.endlessRounds;
        repetition = rounds == 0 ? RepetitionPolicy{SubmitUntilStopped{}} : RepetitionPolicy{SubmitRounds{.count = rounds}};
        if (const auto seconds = config.endlessSeconds; seconds > 0)
        {
            runLimit = std::chrono::seconds{seconds};
        }
    }
    return RunPolicy{orderingFrom(config), concurrency, repetition, runLimit, std::nullopt};
}

}

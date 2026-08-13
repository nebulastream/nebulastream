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

#include <cstddef>
#include <filesystem>
#include <optional>
#include <utility>

#include <Config/Config.hpp>

namespace NES
{

RunPolicy::RunPolicy(
    const OrderingPolicy ordering,
    const size_t concurrency,
    const RepetitionPolicy repetition,
    std::optional<std::filesystem::path> measureReport)
    : ordering(ordering), concurrency(concurrency), repetition(repetition), measureReport(std::move(measureReport))
{
}

RunPolicy RunPolicy::create(const SystestConfiguration& config)
{
    const auto measuring = config.benchmark;
    /// A measuring run submits one query at a time, because queries that run together share the worker and their timings
    /// would depend on each other.
    const auto concurrency = measuring ? 1 : config.numberConcurrentQueries;
    const auto measureReport = measuring ? std::optional{std::filesystem::path{config.workingDir} / "BenchmarkResults.json"} : std::nullopt;

    return RunPolicy{
        config.randomQueryOrder ? OrderingPolicy{RunInShuffledOrder{}} : OrderingPolicy{RunInFileOrder{}},
        concurrency,
        config.endlessMode ? RepetitionPolicy{SubmitUntilStopped{}} : RepetitionPolicy{SubmitOnce{}},
        measureReport};
}

}

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

#include <Config/RunPlan.hpp>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <optional>

#include <Config/Config.hpp>

namespace NES
{

namespace
{

OrderingPolicy orderingFrom(const Config& config)
{
    if (not config.randomQueryOrder.getValue())
    {
        return SourceOrder{};
    }
    /// A zero seed on the command line means none was given, so the run draws one.
    const auto seed = config.shuffleSeed.getValue();
    return Shuffled{.seed = seed == 0 ? std::nullopt : std::optional{seed}};
}

}

RunPlan RunPlan::create(const Config& config)
{
    RunPlan plan{
        .ordering = orderingFrom(config),
        .concurrency = config.numberConcurrentQueries.getValue(),
        .repetition = Once{},
        .runLimit = std::nullopt,
        .measureReport = std::nullopt};

    /// A benchmark takes precedence over endless mode when both are asked for, keeping what the run did before this
    /// plan existed.
    if (config.benchmark.getValue())
    {
        plan.repetition = FixedRounds{.count = std::max(uint64_t{1}, config.benchmarkRounds.getValue())};
        plan.measureReport = std::filesystem::path{config.workingDir.getValue()} / config.benchmarkReport.getValue();
        return plan;
    }

    if (config.endlessMode.getValue())
    {
        const auto rounds = config.endlessRounds.getValue();
        plan.repetition = rounds == 0 ? RepetitionPolicy{UntilLimit{}} : RepetitionPolicy{FixedRounds{.count = rounds}};
        if (const auto seconds = config.endlessSeconds.getValue(); seconds > 0)
        {
            plan.runLimit = std::chrono::seconds{seconds};
        }
    }
    return plan;
}

}

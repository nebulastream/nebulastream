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

#include <filesystem>
#include <optional>

#include <Config/Config.hpp>

namespace NES
{

RunPlan RunPlan::create(const SystestConfiguration& config)
{
    RunPlan plan{
        .ordering = config.randomQueryOrder.getValue() ? OrderingPolicy{Shuffled{}} : OrderingPolicy{SourceOrder{}},
        .concurrency = config.numberConcurrentQueries.getValue(),
        .repetition = config.endlessMode.getValue() ? RepetitionPolicy{UntilStopped{}} : RepetitionPolicy{Once{}},
        .measureReport = std::nullopt};

    if (config.benchmark.getValue())
    {
        plan.measureReport = std::filesystem::path{config.workingDir.getValue()} / "BenchmarkResults.json";
    }
    return plan;
}

}

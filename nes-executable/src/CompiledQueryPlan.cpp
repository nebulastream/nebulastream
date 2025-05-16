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

#include <memory>
#include <ranges>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Util/Ranges.hpp>
#include <CompiledQueryPlan.hpp>
#include <ExecutablePipelineStage.hpp>

namespace NES
{
std::shared_ptr<ExecutablePipeline> ExecutablePipeline::create(
    PipelineId id, std::unique_ptr<ExecutablePipelineStage> stage, const std::vector<std::shared_ptr<ExecutablePipeline>>& successors)
{
    return std::make_shared<ExecutablePipeline>(
        id,
        std::move(stage),
        std::views::transform(successors, [](const auto& strong) { return std::weak_ptr(strong); }) | std::ranges::to<std::vector>());
}

std::unique_ptr<CompiledQueryPlan> CompiledQueryPlan::create(
    QueryId queryId, std::vector<std::shared_ptr<ExecutablePipeline>> pipelines, std::vector<Sink> sinks, std::vector<Source> sources)
{
    return std::make_unique<CompiledQueryPlan>(queryId, std::move(pipelines), std::move(sinks), std::move(sources));
}
}

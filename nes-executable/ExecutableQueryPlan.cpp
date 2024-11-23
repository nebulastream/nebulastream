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

#include <algorithm>
#include <iterator>
#include <memory>
#include <ranges>
#include <utility>
#include <vector>
#include <Util/Ranges.hpp>
#include <ExecutablePipelineStage.hpp>
#include <ExecutableQueryPlan.hpp>

namespace NES::Runtime::Execution
{
std::shared_ptr<ExecutablePipeline> ExecutablePipeline::create(
    std::unique_ptr<ExecutablePipelineStage> stage, const std::vector<std::shared_ptr<ExecutablePipeline>>& successors)
{
    return std::make_shared<ExecutablePipeline>(
        id,
        std::move(stage),
        std::views::transform(successors, [](const auto& strong) { return std::weak_ptr(strong); }) | ranges::to<std::vector>());
}

std::unique_ptr<ExecutableQueryPlan> ExecutableQueryPlan::create(
    QueryId queryId, std::vector<std::shared_ptr<ExecutablePipeline>> pipelines, std::vector<Sink> sinks, std::vector<Source> sources)
{
    return std::make_unique<ExecutableQueryPlan>(queryId, std::move(pipelines), std::move(sinks), std::move(sources));
}
}

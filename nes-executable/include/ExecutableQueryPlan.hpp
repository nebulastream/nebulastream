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

#include <algorithm>
#include <iterator>
#include <memory>
#include <tuple>
#include <utility>
#include <variant>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Sources/SourceHandle.hpp>
#include <Executable.hpp>

namespace NES::Runtime::Execution
{

struct ExecutablePipeline
{
    static std::shared_ptr<ExecutablePipeline>
    create(std::unique_ptr<ExecutablePipelineStage> stage, const std::vector<std::shared_ptr<ExecutablePipeline>>& successors)
    {
        return std::make_shared<ExecutablePipeline>(std::move(stage), successors);
    }

    ExecutablePipeline(std::unique_ptr<ExecutablePipelineStage> stage, const std::vector<std::shared_ptr<ExecutablePipeline>>& successors)
        : stage(std::move(stage))
    {
        std::ranges::transform(successors, std::back_inserter(this->successors), [](const auto& s) { return std::weak_ptr(s); });
    }

    std::unique_ptr<ExecutablePipelineStage> stage;
    std::vector<std::weak_ptr<ExecutablePipeline>> successors;
};

struct ExecutableQueryPlan
{
    static std::unique_ptr<ExecutableQueryPlan> create(
        std::vector<std::shared_ptr<ExecutablePipeline>> pipelines,
        std::vector<std::tuple<OriginId, std::shared_ptr<Sources::SourceDescriptor>, std::vector<std::weak_ptr<ExecutablePipeline>>>>
            sources)
    {
        return std::make_unique<ExecutableQueryPlan>(std::move(pipelines), std::move(sources));
    }

    ExecutableQueryPlan(
        const std::vector<std::shared_ptr<ExecutablePipeline>>& pipelines,
        const std::vector<std::tuple<OriginId, std::shared_ptr<Sources::SourceDescriptor>, std::vector<std::weak_ptr<ExecutablePipeline>>>>&
            sources)
        : pipelines(pipelines), sources(sources)
    {
    }

    std::vector<std::shared_ptr<ExecutablePipeline>> pipelines;
    std::vector<std::tuple<OriginId, std::shared_ptr<Sources::SourceDescriptor>, std::vector<std::weak_ptr<ExecutablePipeline>>>> sources;
};

using ExecutableQueryPlanPtr = std::unique_ptr<ExecutableQueryPlan>;
using ExecutablePipelinePtr = std::shared_ptr<ExecutablePipeline>;

} /// Execution

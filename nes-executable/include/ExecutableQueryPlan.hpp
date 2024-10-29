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
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/SourceDescriptor.hpp>
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

struct Source
{
    std::shared_ptr<Sources::SourceDescriptor> descriptor;
    OriginId originId;
    std::vector<std::weak_ptr<ExecutablePipeline>> successors;
};

struct Sink
{
    std::shared_ptr<Sinks::SinkDescriptor> descriptor;
    std::variant<OriginId, std::weak_ptr<ExecutablePipeline>> predecessor;
};

struct ExecutableQueryPlan
{
    static std::unique_ptr<ExecutableQueryPlan>
    create(std::vector<std::shared_ptr<ExecutablePipeline>> pipelines, std::vector<Sink> sinks, std::vector<Source> sources)
    {
        return std::make_unique<ExecutableQueryPlan>(std::move(pipelines), std::move(sinks), std::move(sources));
    }

    ExecutableQueryPlan(std::vector<std::shared_ptr<ExecutablePipeline>> pipelines, std::vector<Sink> sinks, std::vector<Source> sources)
        : sinks(std::move(sinks)), sources(std::move(sources)), pipelines(std::move(pipelines))
    {
    }

    std::vector<Sink> sinks;
    std::vector<Source> sources;
    std::vector<std::shared_ptr<ExecutablePipeline>> pipelines;
};

using ExecutableQueryPlanPtr = std::unique_ptr<ExecutableQueryPlan>;
using ExecutablePipelinePtr = std::shared_ptr<ExecutablePipeline>;

} /// Execution

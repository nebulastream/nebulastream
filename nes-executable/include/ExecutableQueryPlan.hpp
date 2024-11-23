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

#include <memory>
#include <variant>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Executable.hpp>

namespace NES::Runtime::Execution
{

struct ExecutablePipeline
{
    static std::shared_ptr<ExecutablePipeline>
    create(std::unique_ptr<ExecutablePipelineStage> stage, const std::vector<std::shared_ptr<ExecutablePipeline>>& successors);

    ExecutablePipeline(std::unique_ptr<ExecutablePipelineStage> stage, const std::vector<std::shared_ptr<ExecutablePipeline>>& successors);

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
    std::vector<std::variant<OriginId, std::weak_ptr<ExecutablePipeline>>> predecessor;
};

struct ExecutableQueryPlan
{
    static std::unique_ptr<ExecutableQueryPlan> create(
        QueryId queryId, std::vector<std::shared_ptr<ExecutablePipeline>> pipelines, std::vector<Sink> sinks, std::vector<Source> sources);

    ExecutableQueryPlan(
        QueryId queryId, std::vector<std::shared_ptr<ExecutablePipeline>> pipelines, std::vector<Sink> sinks, std::vector<Source> sources);

    QueryId queryId;
    std::vector<Sink> sinks;
    std::vector<Source> sources;
    std::vector<std::shared_ptr<ExecutablePipeline>> pipelines;
};

using ExecutableQueryPlanPtr = std::unique_ptr<ExecutableQueryPlan>;
using ExecutablePipelinePtr = std::shared_ptr<ExecutablePipeline>;

} /// Execution

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
#include <ExecutablePipelineStage.hpp>

namespace NES
{

struct ExecutablePipeline
{
    static std::shared_ptr<ExecutablePipeline> create(
        PipelineId id, std::unique_ptr<ExecutablePipelineStage> stage, const std::vector<std::shared_ptr<ExecutablePipeline>>& successors);

    PipelineId id;
    std::unique_ptr<ExecutablePipelineStage> stage;
    std::vector<std::weak_ptr<ExecutablePipeline>> successors;
};

struct Source
{
    /// The Source representation in the `CompiledQueryPlan` is still an abstract source representation. During Query Instantiation
    /// the descriptor and originId are instantiated into concrete source implementation.
    OriginId originId;
    std::shared_ptr<Sources::SourceDescriptor> descriptor;

    /// Sources do not have any predecessors
    std::vector<std::weak_ptr<ExecutablePipeline>> successors;
};

struct Sink
{
    /// The Sink representation in the `CompiledQueryPlan` is still an abstract sink representation. During Query Instantiation
    /// the descriptor is instantiated into concrete sink implementation.
    std::shared_ptr<Sinks::SinkDescriptor> descriptor;

    /// Sinks do not have any successors
    std::vector<std::variant<OriginId, std::weak_ptr<ExecutablePipeline>>> predecessor;
};

struct CompiledQueryPlan
{
    static std::unique_ptr<CompiledQueryPlan> create(
        QueryId queryId, std::vector<std::shared_ptr<ExecutablePipeline>> pipelines, std::vector<Sink> sinks, std::vector<Source> sources);

    QueryId queryId;
    std::vector<std::shared_ptr<ExecutablePipeline>> pipelines;
    std::vector<Sink> sinks;
    std::vector<Source> sources;
};
}

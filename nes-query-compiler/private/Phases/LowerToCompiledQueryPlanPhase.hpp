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
#include <unordered_map>
#include <variant>
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <Util/DumpMode.hpp>
#include <CompiledQueryPlan.hpp>
#include <PipelinedQueryPlan.hpp>

namespace NES
{
class LowerToCompiledQueryPlanPhase
{
public:
    explicit LowerToCompiledQueryPlanPhase(DumpMode dumpQueryCompilationIntermediateRepresentations)
        : dumpQueryCompilationIR(dumpQueryCompilationIntermediateRepresentations)
    {
    }

    std::unique_ptr<CompiledQueryPlan> apply(const std::shared_ptr<PipelinedQueryPlan>& pipelineQueryPlan);

private:
    using Predecessor = std::variant<OperatorId, std::weak_ptr<ExecutablePipeline>>;
    using Successor = std::optional<std::shared_ptr<ExecutablePipeline>>;

    std::shared_ptr<ExecutablePipeline> processOperatorPipeline(const std::shared_ptr<Pipeline>& pipeline);
    void processSink(const Predecessor& predecessor, const std::shared_ptr<Pipeline>& pipeline);
    Successor processSuccessor(const Predecessor& predecessor, const std::shared_ptr<Pipeline>& pipeline);
    void processSource(const std::shared_ptr<Pipeline>& pipeline);

    std::unique_ptr<ExecutablePipelineStage> getStage(const std::shared_ptr<Pipeline>& pipeline);

    /// Lowering context
    std::vector<CompiledQueryPlan::Sink> sinks;
    std::vector<CompiledQueryPlan::Source> sources;
    std::unordered_map<PipelineId, std::shared_ptr<ExecutablePipeline>> pipelineToExecutableMap;

    std::shared_ptr<PipelinedQueryPlan> pipelineQueryPlan;

    /// Config parameter
    DumpMode dumpQueryCompilationIR;
};
}

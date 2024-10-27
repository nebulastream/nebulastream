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

#include <vector>
#include <QueryCompiler/Phases/Translations/DataSinkProvider.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Sources/SourceHandle.hpp>
#include <Sources/SourceProvider.hpp>
#include <ExecutableQueryPlan.hpp>

namespace NES
{

class PhysicalSourceType;
using PhysicalSourceTypePtr = std::shared_ptr<PhysicalSourceType>;

namespace QueryCompilation
{

class LowerToExecutableQueryPlanPhase
{
public:
    LowerToExecutableQueryPlanPhase(DataSinkProviderPtr sinkProvider);
    static std::shared_ptr<LowerToExecutableQueryPlanPhase> create(const DataSinkProviderPtr& sinkProvider);

    Runtime::Execution::ExecutableQueryPlanPtr apply(const PipelineQueryPlanPtr& pipelineQueryPlan);

private:
    DataSinkProviderPtr sinkProvider;
    using Sources = std::vector<std::tuple<
        OriginId,
        std::shared_ptr<Sources::SourceDescriptor>,
        std::vector<std::weak_ptr<Runtime::Execution::ExecutablePipeline>>>>;
    void processSource(
        const OperatorPipelinePtr& pipeline,
        Sources& sources,
        std::vector<Runtime::Execution::ExecutablePipelinePtr>& executablePipelines,
        const PipelineQueryPlanPtr& pipelineQueryPlan,
        std::map<PipelineId, Runtime::Execution::ExecutablePipelinePtr>& pipelineToExecutableMap);

    Runtime::Execution::ExecutablePipelinePtr processSuccessor(
        const OperatorPipelinePtr& pipeline,
        Sources& sources,
        std::vector<Runtime::Execution::ExecutablePipelinePtr>& executablePipelines,
        const PipelineQueryPlanPtr& pipelineQueryPlan,
        std::map<PipelineId, Runtime::Execution::ExecutablePipelinePtr>& pipelineToExecutableMap);

    Runtime::Execution::ExecutablePipelinePtr processSink(
        const OperatorPipelinePtr& pipeline,
        Sources& sources,
        std::vector<Runtime::Execution::ExecutablePipelinePtr>& executablePipelines,
        const PipelineQueryPlanPtr& pipelineQueryPlan);

    Runtime::Execution::ExecutablePipelinePtr processOperatorPipeline(
        const OperatorPipelinePtr& pipeline,
        Sources& sources,
        std::vector<Runtime::Execution::ExecutablePipelinePtr>& executablePipelines,
        const PipelineQueryPlanPtr& pipelineQueryPlan,
        std::map<PipelineId, Runtime::Execution::ExecutablePipelinePtr>& pipelineToExecutableMap);
};

}
}

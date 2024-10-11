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
#include <Runtime/Execution/ExecutablePipeline.hpp>
#include <Runtime/Execution/ExecutableQueryPlan.hpp>
#include <Sources/SourceHandle.hpp>
#include <Sources/SourceProvider.hpp>

namespace NES
{

class PhysicalSourceType;
using PhysicalSourceTypePtr = std::shared_ptr<PhysicalSourceType>;

namespace QueryCompilation
{

class LowerToExecutableQueryPlanPhase
{
public:
    LowerToExecutableQueryPlanPhase(DataSinkProviderPtr sinkProvider, std::unique_ptr<Sources::SourceProvider>&& sourceProvider);
    static std::shared_ptr<LowerToExecutableQueryPlanPhase>
    create(const DataSinkProviderPtr& sinkProvider, std::unique_ptr<Sources::SourceProvider>&& sourceProvider);

    Runtime::Execution::ExecutableQueryPlanPtr
    apply(const PipelineQueryPlanPtr& pipelineQueryPlan, const Runtime::NodeEnginePtr& nodeEngine);

private:
    DataSinkProviderPtr sinkProvider;
    std::unique_ptr<Sources::SourceProvider> sourceProvider;
    void processSource(
        const OperatorPipelinePtr& pipeline,
        std::vector<std::unique_ptr<Sources::SourceHandle>>& sources,
        std::vector<DataSinkPtr>& sinks,
        std::vector<Runtime::Execution::ExecutablePipelinePtr>& executablePipelines,
        const Runtime::NodeEnginePtr& nodeEngine,
        const PipelineQueryPlanPtr& pipelineQueryPlan,
        std::map<PipelineId, Runtime::Execution::SuccessorExecutablePipeline>& pipelineToExecutableMap);

    Runtime::Execution::SuccessorExecutablePipeline processSuccessor(
        const OperatorPipelinePtr& pipeline,
        std::vector<std::unique_ptr<Sources::SourceHandle>>& sources,
        std::vector<DataSinkPtr>& sinks,
        std::vector<Runtime::Execution::ExecutablePipelinePtr>& executablePipelines,
        const Runtime::NodeEnginePtr& nodeEngine,
        const PipelineQueryPlanPtr& pipelineQueryPlan,
        std::map<PipelineId, Runtime::Execution::SuccessorExecutablePipeline>& pipelineToExecutableMap);

    Runtime::Execution::SuccessorExecutablePipeline processSink(
        const OperatorPipelinePtr& pipeline,
        std::vector<DataSinkPtr>& sinks,
        Runtime::NodeEnginePtr nodeEngine,
        const PipelineQueryPlanPtr& pipelineQueryPlan);

    Runtime::Execution::SuccessorExecutablePipeline processOperatorPipeline(
        const OperatorPipelinePtr& pipeline,
        std::vector<std::unique_ptr<Sources::SourceHandle>>& sources,
        std::vector<DataSinkPtr>& sinks,
        std::vector<Runtime::Execution::ExecutablePipelinePtr>& executablePipelines,
        const Runtime::NodeEnginePtr& nodeEngine,
        const PipelineQueryPlanPtr& pipelineQueryPlan,
        std::map<PipelineId, Runtime::Execution::SuccessorExecutablePipeline>& pipelineToExecutableMap);
};

}
}

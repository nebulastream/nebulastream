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


#include <unordered_set>
#include <vector>
#include <Runtime/Execution/ExecutablePipeline.hpp>
#include <Runtime/Execution/ExecutableQueryPlan.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkProvider.hpp>
#include <Sources/SourceHandle.hpp>
#include <Sources/SourceProvider.hpp>

namespace NES
{

namespace QueryCompilation
{

class LowerToExecutableQueryPlanPhase
{
public:
    static Runtime::Execution::ExecutableQueryPlanPtr
    apply(const PipelineQueryPlanPtr& pipelineQueryPlan, const Runtime::NodeEnginePtr& nodeEngine);

private:
    static void processSource(
        const OperatorPipelinePtr& pipeline,
        std::vector<std::unique_ptr<Sources::SourceHandle>>& sources,
        std::unordered_set<std::shared_ptr<NES::Sinks::Sink>>& sinks,
        std::vector<Runtime::Execution::ExecutablePipelinePtr>& executablePipelines,
        const Runtime::NodeEnginePtr& nodeEngine,
        const PipelineQueryPlanPtr& pipelineQueryPlan,
        std::map<PipelineId, Runtime::Execution::SuccessorExecutablePipeline>& pipelineToExecutableMap);

    static Runtime::Execution::SuccessorExecutablePipeline processSuccessor(
        const OperatorPipelinePtr& pipeline,
        std::vector<std::unique_ptr<Sources::SourceHandle>>& sources,
        std::unordered_set<std::shared_ptr<NES::Sinks::Sink>>& sinks,
        std::vector<Runtime::Execution::ExecutablePipelinePtr>& executablePipelines,
        const Runtime::NodeEnginePtr& nodeEngine,
        const PipelineQueryPlanPtr& pipelineQueryPlan,
        std::map<PipelineId, Runtime::Execution::SuccessorExecutablePipeline>& pipelineToExecutableMap);

    static Runtime::Execution::SuccessorExecutablePipeline
    processSink(const OperatorPipelinePtr& pipeline, std::unordered_set<std::shared_ptr<NES::Sinks::Sink>>& sinks, QueryId queryId);

    static Runtime::Execution::SuccessorExecutablePipeline processOperatorPipeline(
        const OperatorPipelinePtr& pipeline,
        std::vector<std::unique_ptr<Sources::SourceHandle>>& sources,
        std::unordered_set<std::shared_ptr<NES::Sinks::Sink>>& sinks,
        std::vector<Runtime::Execution::ExecutablePipelinePtr>& executablePipelines,
        const std::shared_ptr<Runtime::NodeEngine>& nodeEngine,
        const PipelineQueryPlanPtr& pipelineQueryPlan,
        std::map<PipelineId, Runtime::Execution::SuccessorExecutablePipeline>& pipelineToExecutableMap);
};

}
}

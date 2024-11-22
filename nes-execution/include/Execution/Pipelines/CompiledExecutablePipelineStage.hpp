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
#include <future>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Util/Timer.hpp>
#include <nautilus/Engine.hpp>
namespace NES
{
class DumpHelper;
}
namespace NES::Nautilus::Backends
{
class Executable;
}
namespace NES::Nautilus::IR
{
class IRGraph;
}
namespace NES::Runtime::Execution
{
class PhysicalOperatorPipeline;

/**
 * @brief A compiled executable pipeline stage uses nautilus-lib to compile a pipeline to a code snippet.
 */
class CompiledExecutablePipelineStage final : public ExecutablePipelineStage
{
public:
    CompiledExecutablePipelineStage(
        const std::shared_ptr<PhysicalOperatorPipeline>& physicalOperatorPipeline,
        std::vector<std::shared_ptr<OperatorHandler>> operatorHandler,
        nautilus::engine::Options options);
    uint32_t start(PipelineExecutionContext& pipelineExecutionContext) override;
    void execute(const Memory::TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext) override;
    uint32_t stop(PipelineExecutionContext& pipelineExecutionContext) override;

private:
    [[nodiscard]] nautilus::engine::CallableFunction<void, WorkerContext*, PipelineExecutionContext*, Memory::TupleBuffer*>
    compilePipeline() const;
    const nautilus::engine::Options options;
    nautilus::engine::CallableFunction<void, PipelineExecutionContext*, const Memory::TupleBuffer*> compiledPipelineFunction;
    std::vector<OperatorHandlerPtr> operatorHandlers;
    std::shared_ptr<PhysicalOperatorPipeline> physicalOperatorPipeline;
};

}

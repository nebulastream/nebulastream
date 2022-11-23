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
#ifndef NES_RUNTIME_INCLUDE_EXECUTION_PIPELINES_COMPILEDEXECUTABLEPIPELINESTAGE_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_PIPELINES_COMPILEDEXECUTABLEPIPELINESTAGE_HPP_
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Pipelines/NautilusExecutablePipelineStage.hpp>
#include <Nautilus/Backends/Executable.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <future>

namespace NES::Runtime::Execution {
class PhysicalOperatorPipeline;

/**
 * @brief A compiled executable pipeline stage uses nautilus to compile a pipeline to a code snippit.
 */
class CompiledExecutablePipelineStage : public NautilusExecutablePipelineStage {
  public:
    CompiledExecutablePipelineStage(std::shared_ptr<PhysicalOperatorPipeline> physicalOperatorPipeline);
    uint32_t setup(PipelineExecutionContext& pipelineExecutionContext) override;
    ExecutionResult execute(TupleBuffer& inputTupleBuffer,
                            PipelineExecutionContext& pipelineExecutionContext,
                            WorkerContext& workerContext) override;

  private:
    std::unique_ptr<Nautilus::Backends::Executable> compilePipeline();
    std::shared_future<std::unique_ptr<Nautilus::Backends::Executable>> executablePipeline;
};

}// namespace NES::Runtime::Execution

#endif// NES_RUNTIME_INCLUDE_EXECUTION_PIPELINES_COMPILEDEXECUTABLEPIPELINESTAGE_HPP_

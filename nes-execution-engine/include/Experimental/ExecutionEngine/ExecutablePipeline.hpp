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
#ifndef NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_EXECUTIONENGINE_EXECUTABLEPIPELINE_HPP_
#define NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_EXECUTIONENGINE_EXECUTABLEPIPELINE_HPP_
#include <Experimental/Runtime/RuntimeExecutionContext.hpp>
#include <Experimental/Runtime/RuntimePipelineContext.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <mlir/ExecutionEngine/ExecutionEngine.h>

namespace NES::ExecutionEngine::Experimental {
class PhysicalOperatorPipeline;
class ExecutablePipeline {
  public:
    ExecutablePipeline(std::shared_ptr<Runtime::Execution::RuntimePipelineContext> executionContext,
                       std::shared_ptr<PhysicalOperatorPipeline> physicalOperatorPipeline,
                       std::unique_ptr<mlir::ExecutionEngine> engine);
    ~ExecutablePipeline() = default;
    void setup();
    void execute(Runtime::WorkerContext& workerContext, Runtime::TupleBuffer& buffer);

    std::shared_ptr<Runtime::Execution::RuntimePipelineContext> getExecutionContext();
  private:
    std::shared_ptr<Runtime::Execution::RuntimePipelineContext> executionContext;
    std::shared_ptr<PhysicalOperatorPipeline> physicalOperatorPipeline;
    std::unique_ptr<mlir::ExecutionEngine> engine;
};

}// namespace NES::ExecutionEngine::Experimental

#endif//NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_EXECUTIONENGINE_EXECUTABLEPIPELINE_HPP_

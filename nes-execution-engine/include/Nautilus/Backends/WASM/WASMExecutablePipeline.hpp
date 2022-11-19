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

#ifndef NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_WASM_WASMEXECUTIONPIPELINE_HPP_
#define NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_WASM_WASMEXECUTIONPIPELINE_HPP_
#include <Experimental/ExecutionEngine/ExecutablePipeline.hpp>
#include <Experimental/ExecutionEngine/PhysicalOperatorPipeline.hpp>
#include <Experimental/Runtime/RuntimePipelineContext.hpp>
#include <Nautilus/Backends/WASM/WASMRuntime.hpp>
#include <mlir/ExecutionEngine/ExecutionEngine.h>

namespace NES::Nautilus::Backends::WASM {

class WASMExecutablePipeline : public ExecutionEngine::Experimental::ExecutablePipeline {
  public:
    WASMExecutablePipeline(std::shared_ptr<Runtime::Execution::RuntimePipelineContext> executionContext,
                           std::shared_ptr<ExecutionEngine::Experimental::PhysicalOperatorPipeline> physicalOperatorPipeline,
                           std::unique_ptr<mlir::ExecutionEngine> engine);
    void setup() override;
    void execute(Runtime::WorkerContext& workerContext, Runtime::TupleBuffer& buffer) override;

  private:
    //TODO: include wasmtime engine here
    std::unique_ptr<mlir::ExecutionEngine> engine;
    std::unique_ptr<WASMRuntime> engine2;
};
}// namespace NES::Nautilus::Backends::WASM

#endif//NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_WASM_WASMEXECUTIONPIPELINE_HPP_
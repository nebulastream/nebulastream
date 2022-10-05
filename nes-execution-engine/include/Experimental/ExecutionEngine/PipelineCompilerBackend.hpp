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
#ifndef NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_EXECUTIONENGINE_PIPELINECOMPILERBACKEND_HPP_
#define NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_EXECUTIONENGINE_PIPELINECOMPILERBACKEND_HPP_
#include <Util/Timer.hpp>
#include <Experimental/ExecutionEngine/PhysicalOperatorPipeline.hpp>
#include <Experimental/Interpreter/ExecutionContext.hpp>
#include <Experimental/Runtime/RuntimePipelineContext.hpp>
#include <Nautilus/IR/IRGraph.hpp>
#include <memory>
#include <Nautilus/Backends/MLIR/LLVMIROptimizer.hpp>

namespace NES::Nautilus::IR {
class IRGraph;
}
namespace NES::ExecutionEngine::Experimental {
class ExecutablePipeline;
class PipelineCompilerBackend {
  public:
    virtual ~PipelineCompilerBackend() = default;
    virtual std::shared_ptr<ExecutablePipeline>
    compile(std::shared_ptr<Runtime::Execution::RuntimePipelineContext> executionContext,
            std::shared_ptr<PhysicalOperatorPipeline> physicalOperatorPipeline,
            std::shared_ptr<Nautilus::IR::IRGraph> ir,
            std::shared_ptr<Timer<>> timer,
            Nautilus::Backends::MLIR::LLVMIROptimizer::OptimizationLevel optLevel = Nautilus::Backends::MLIR::LLVMIROptimizer::OptimizationLevel::O3, 
            bool inlining = false) = 0;
};

}// namespace NES::ExecutionEngine::Experimental

#endif//NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_EXECUTIONENGINE_PIPELINECOMPILERBACKEND_HPP_

#ifndef NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_EXECUTIONENGINE_PIPELINECOMPILERBACKEND_HPP_
#define NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_EXECUTIONENGINE_PIPELINECOMPILERBACKEND_HPP_
#include <Experimental/ExecutionEngine/PhysicalOperatorPipeline.hpp>
#include <Experimental/Interpreter/ExecutionContext.hpp>
#include <Experimental/NESIR/NESIR.hpp>
#include <Experimental/Runtime/RuntimePipelineContext.hpp>
#include <memory>
namespace NES::ExecutionEngine::Experimental::IR {
class NESIR;
}
namespace NES::ExecutionEngine::Experimental {
class ExecutablePipeline;
class PipelineCompilerBackend {
  public:
    virtual ~PipelineCompilerBackend() = default;
    virtual std::shared_ptr<ExecutablePipeline>
    compile(std::shared_ptr<Runtime::Execution::RuntimePipelineContext> executionContext,
            std::shared_ptr<PhysicalOperatorPipeline> physicalOperatorPipeline,
            std::shared_ptr<IR::NESIR> ir) = 0;
};

}// namespace NES::ExecutionEngine::Experimental

#endif//NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_EXECUTIONENGINE_PIPELINECOMPILERBACKEND_HPP_

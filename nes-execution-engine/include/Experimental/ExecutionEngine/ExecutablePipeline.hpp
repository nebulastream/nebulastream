#ifndef NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_EXECUTIONENGINE_EXECUTABLEPIPELINE_HPP_
#define NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_EXECUTIONENGINE_EXECUTABLEPIPELINE_HPP_
#include <Experimental/Runtime/ExecutionContext.hpp>
#include <Experimental/Runtime/PipelineContext.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <mlir/ExecutionEngine/ExecutionEngine.h>

namespace NES::ExecutionEngine::Experimental {

class ExecutablePipeline {
  public:
    ExecutablePipeline(std::shared_ptr<Runtime::Execution::PipelineContext> executionContext,
                       std::unique_ptr<mlir::ExecutionEngine> engine);
    ~ExecutablePipeline() = default;
    void execute(Runtime::WorkerContext& workerContext, Runtime::TupleBuffer& buffer);

  private:
    std::shared_ptr<Runtime::Execution::PipelineContext> executionContext;
    std::unique_ptr<mlir::ExecutionEngine> engine;
};

}// namespace NES::ExecutionEngine::Experimental

#endif//NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_EXECUTIONENGINE_EXECUTABLEPIPELINE_HPP_

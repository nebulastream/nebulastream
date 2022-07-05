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

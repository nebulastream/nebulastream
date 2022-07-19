#ifndef NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_MLIR_MLIREXECUTABLEPIPELINE_HPP_
#define NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_MLIR_MLIREXECUTABLEPIPELINE_HPP_
#include <Experimental/ExecutionEngine/ExecutablePipeline.hpp>
#include <Experimental/ExecutionEngine/PhysicalOperatorPipeline.hpp>
#include <Experimental/Runtime/RuntimePipelineContext.hpp>
#include <mlir/ExecutionEngine/ExecutionEngine.h>

namespace NES::ExecutionEngine::Experimental {

class MLIRExecutablePipeline : public ExecutablePipeline {
  public:
    MLIRExecutablePipeline(std::shared_ptr<Runtime::Execution::RuntimePipelineContext> executionContext,
                           std::shared_ptr<PhysicalOperatorPipeline> physicalOperatorPipeline,
                           std::unique_ptr<mlir::ExecutionEngine> engine);
    void setup() override;
    void execute(Runtime::WorkerContext& workerContext, Runtime::TupleBuffer& buffer) override;

  private:
    std::unique_ptr<mlir::ExecutionEngine> engine;
};

}// namespace NES::ExecutionEngine::Experimental

#endif//NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_MLIR_MLIREXECUTABLEPIPELINE_HPP_

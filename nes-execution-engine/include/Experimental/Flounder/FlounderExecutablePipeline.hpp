#ifndef NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_MLIR_FlounderExecutablePipeline_HPP_
#define NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_MLIR_FlounderExecutablePipeline_HPP_
#include <Experimental/ExecutionEngine/ExecutablePipeline.hpp>
#include <Experimental/ExecutionEngine/PhysicalOperatorPipeline.hpp>
#include <Experimental/Runtime/RuntimePipelineContext.hpp>
#include <flounder/executable.h>

namespace NES::ExecutionEngine::Experimental {

class FlounderExecutablePipeline : public ExecutablePipeline {
  public:
    FlounderExecutablePipeline(std::shared_ptr<Runtime::Execution::RuntimePipelineContext> executionContext,
                               std::shared_ptr<PhysicalOperatorPipeline> physicalOperatorPipeline,
                               std::unique_ptr<flounder::Executable> engine);
    void setup() override;
    void execute(Runtime::WorkerContext& workerContext, Runtime::TupleBuffer& buffer) override;

  private:
    std::unique_ptr<flounder::Executable> engine;
};

}// namespace NES::ExecutionEngine::Experimental

#endif//NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_MLIR_FlounderExecutablePipeline_HPP_

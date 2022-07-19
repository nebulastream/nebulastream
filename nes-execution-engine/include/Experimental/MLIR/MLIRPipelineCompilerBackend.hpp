#ifndef NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_MLIR_MLIRPIPELINECOMPILERBACKEND_HPP_
#define NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_MLIR_MLIRPIPELINECOMPILERBACKEND_HPP_
#include <Experimental/ExecutionEngine/PipelineCompilerBackend.hpp>
namespace NES::ExecutionEngine::Experimental {
class MLIRPipelineCompilerBackend : public PipelineCompilerBackend {
  public:
    std::shared_ptr<ExecutablePipeline> compile(std::shared_ptr<Runtime::Execution::RuntimePipelineContext> executionContext,
                                                std::shared_ptr<PhysicalOperatorPipeline> physicalOperatorPipeline,
                                                std::shared_ptr<IR::NESIR> ir) override;
};

}// namespace NES::ExecutionEngine::Experimental

#endif//NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_MLIR_MLIRPIPELINECOMPILERBACKEND_HPP_

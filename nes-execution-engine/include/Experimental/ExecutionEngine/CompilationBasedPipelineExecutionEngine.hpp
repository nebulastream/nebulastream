#ifndef NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_EXECUTIONENGINE_COMPILATIONBASEDEXECUTIONENGINE_HPP_
#define NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_EXECUTIONENGINE_COMPILATIONBASEDEXECUTIONENGINE_HPP_
#include <Experimental/ExecutionEngine/PipelineExecutionEngine.hpp>
namespace NES::ExecutionEngine::Experimental {

class CompilationBasedPipelineExecutionEngine : public PipelineExecutionEngine {

  public:
    std::shared_ptr<ExecutablePipeline> compile(std::shared_ptr<PhysicalOperatorPipeline> physicalOperatorPipeline) override;
};

}// namespace NES::ExecutionEngine::Experimental

#endif//NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_EXECUTIONENGINE_COMPILATIONBASEDEXECUTIONENGINE_HPP_

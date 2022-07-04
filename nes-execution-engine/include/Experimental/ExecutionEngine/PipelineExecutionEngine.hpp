#ifndef NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_EXECUTIONENGINE_EXECUTIONENGINE_HPP_
#define NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_EXECUTIONENGINE_EXECUTIONENGINE_HPP_
#include <memory>
namespace NES::ExecutionEngine::Experimental {

class ExecutablePipeline;
class PhysicalOperatorPipeline;

class PipelineExecutionEngine {

  public:
    /**
     * @brief Compiles a physical operator pipeline to an executable pipeline.
     * Depending on the backend this may involve code generation and compilation
     * @param physicalOperatorPipeline
     * @return ExecutablePipeline
     */
    virtual std::shared_ptr<ExecutablePipeline> compile(std::shared_ptr<PhysicalOperatorPipeline> physicalOperatorPipeline) = 0;
};

}// namespace NES::ExecutionEngine::Experimental
#endif//NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_EXECUTIONENGINE_EXECUTIONENGINE_HPP_

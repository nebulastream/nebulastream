#ifndef NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_RUNTIME_PIPELINECONTEXT_HPP_
#define NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_RUNTIME_PIPELINECONTEXT_HPP_
#include <Runtime/TupleBuffer.hpp>
#include <Experimental/Interpreter/ExecutionContext.hpp>
#include <memory>

namespace NES::Runtime::Execution {

/**
 * @brief The PipelineExecutionContext is passed to a compiled pipeline and offers basic functionality to interact with the Runtime.
 * Via the context, the compile code is able to allocate new TupleBuffers and to emit tuple buffers to the Runtime.
 */
class PipelineContext : public std::enable_shared_from_this<PipelineContext> {
  public:
    using OperatorStateTag = uint32_t;
    PipelineContext::OperatorStateTag
    registerGlobalOperatorState(std::unique_ptr<ExecutionEngine::Experimental::Interpreter::OperatorState> operatorState);
    ExecutionEngine::Experimental::Interpreter::OperatorState* getGlobalOperatorState(PipelineContext::OperatorStateTag tag);
    void dispatchBuffer(Runtime::TupleBuffer& tb);

  private:
    std::vector<std::unique_ptr<ExecutionEngine::Experimental::Interpreter::OperatorState>> operatorStates;
};

}// namespace NES::Runtime::Execution

#endif//NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_RUNTIME_PIPELINECONTEXT_HPP_

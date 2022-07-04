#ifndef NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_INTERPRETER_WORKERCONTEXT_HPP_
#define NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_INTERPRETER_WORKERCONTEXT_HPP_
#include <Experimental/Interpreter/DataValue/Value.hpp>
namespace NES::ExecutionEngine::Experimental::Interpreter {

class WorkerContext {
  public:
    WorkerContext(Value<MemRef> workerContextRef);
    Value<Integer> getWorkerId();
    Value<MemRef> allocateBuffer();
  private:
    Value<MemRef> workerContextRef;
};

}// namespace NES::ExecutionEngine::Experimental::Interpreter
#endif//NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_INTERPRETER_WORKERCONTEXT_HPP_

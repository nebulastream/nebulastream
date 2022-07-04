#ifndef NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_INTERPRETER_PIPELINECONTEXT_HPP_
#define NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_INTERPRETER_PIPELINECONTEXT_HPP_

#include <Experimental/Interpreter/DataValue/Value.hpp>
namespace NES::ExecutionEngine::Experimental::Interpreter {
class RecordBuffer;
class Operator;
class OperatorState;
class PipelineContext {
  public:
    PipelineContext(Value<MemRef> pipelineContextRef);

    void emitBuffer(const RecordBuffer& rb);

    void registerGlobalOperatorState(const Operator* operatorPtr, std::unique_ptr<OperatorState> operatorState);

    Value<MemRef> getGlobalOperatorState(const Operator* operatorPtr);

  private:
    Value<MemRef> pipelineContextRef;
    std::unordered_map<const Operator*, uint32_t> operatorIndexMap;
};

}// namespace NES::ExecutionEngine::Experimental::Interpreter
#endif//NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_INTERPRETER_PIPELINECONTEXT_HPP_

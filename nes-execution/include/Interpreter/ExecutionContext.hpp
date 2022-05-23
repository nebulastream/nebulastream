#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_EXECUTIONCONTEXT_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_EXECUTIONCONTEXT_HPP_
#include <Interpreter/DataValue/MemRef.hpp>
#include <Interpreter/DataValue/Value.hpp>
#include <memory>
#include <unordered_map>
namespace NES::Interpreter {
class Operator;
class RecordBuffer;
class OperatorState {
  public:
    virtual ~OperatorState() = default;
};
class ExecutionContext {
  public:
    ExecutionContext(Value<MemRef> pipelineContext, Value<MemRef> workerContext);
    void setLocalOperatorState(const Operator* op, std::unique_ptr<OperatorState> state);
    OperatorState* getLocalState(const Operator* op);
    void emitBuffer(const RecordBuffer& rb);
    Value<MemRef> allocateBuffer();

  private:
    std::unordered_map<const Operator*, std::unique_ptr<OperatorState>> localStateMap;
    Value<MemRef> pipelineContext;
    Value<MemRef> workerContext;
};

}// namespace NES::Interpreter

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_EXECUTIONCONTEXT_HPP_

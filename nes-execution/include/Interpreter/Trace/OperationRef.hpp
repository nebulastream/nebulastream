#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_TRACE_OPERATIONREF_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_TRACE_OPERATIONREF_HPP_
#include <cinttypes>
namespace NES::Interpreter {
class OperationRef {
  public:
    OperationRef(uint32_t blockId, uint32_t operationId) : blockId(blockId), operationId(operationId) {}
    uint32_t blockId;
    uint32_t operationId;
};
}// namespace NES::Interpreter

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_TRACE_OPERATIONREF_HPP_

#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_SCAN_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_SCAN_HPP_
#include <Experimental/Interpreter/Operators/Operator.hpp>
#include <Experimental/Interpreter/RecordBuffer.hpp>

namespace NES::Interpreter {

class Scan : public Operator {
  public:
    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
};

}// namespace NES::Interpreter
#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_SCAN_HPP_

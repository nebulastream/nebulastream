#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_EMIT_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_EMIT_HPP_
#include <Interpreter/Operators/ExecuteOperator.hpp>

namespace NES::Interpreter {

class Emit : public ExecuteOperator {
  public:
    void execute(Record& record) override;
};

}// namespace NES::Interpreter
#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_EMIT_HPP_

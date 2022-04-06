#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_SCAN_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_SCAN_HPP_
#include <Interpreter/Operators/Operator.hpp>

namespace NES::Interpreter {

class Scan: public Operator{
  public:
    void open(TraceContext& tracer) override;
};

}
#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_SCAN_HPP_

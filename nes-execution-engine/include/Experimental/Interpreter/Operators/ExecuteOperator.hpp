#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_EXECUTEOPERATOR_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_EXECUTEOPERATOR_HPP_
#include <Experimental/Interpreter/Operators/Operator.hpp>
namespace NES::Interpreter {

class Record;

class ExecuteOperator : public Operator {
  public:
    virtual void execute(ExecutionContext& ctx, Record& record) const = 0;
    virtual ~ExecuteOperator() = default;
};

}// namespace NES::Interpreter

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_EXECUTEOPERATOR_HPP_

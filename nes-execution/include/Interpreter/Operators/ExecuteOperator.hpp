#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_EXECUTEOPERATOR_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_EXECUTEOPERATOR_HPP_
#include <Interpreter/Operators/Operator.hpp>
namespace NES::Interpreter {

class Record;

class ExecuteOperator : public Operator {
  public:
    virtual void open() = 0;
    virtual void execute(Record& record) = 0;
    virtual ~ExecuteOperator() = default;
};

}// namespace NES::Interpreter

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_EXECUTEOPERATOR_HPP_

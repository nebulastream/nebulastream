#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_OPERATOR_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_OPERATOR_HPP_
#include <memory>
namespace NES::Interpreter {
class ExecuteOperator;
using ExecuteOperatorPtr = std::shared_ptr<ExecuteOperator>;
class TraceContext;
class Operator {
  public:
    virtual void open(TraceContext& tracer) = 0;
    void setChild(ExecuteOperatorPtr child) { this->child = std::move(child); }
    virtual ~Operator() {};
  protected:
    ExecuteOperatorPtr child;
};

}// namespace NES::Interpreter

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_OPERATOR_HPP_

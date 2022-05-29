#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_OPERATOR_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_OPERATOR_HPP_
#include <memory>

namespace NES::Interpreter {
class RecordBuffer;
class ExecuteOperator;
class ExecutionContext;
using ExecuteOperatorPtr = std::shared_ptr<ExecuteOperator>;
class TraceContext;
class Operator {
  public:
    /**
     * @brief Open is called for each record buffer and is used to initializes execution local state.
     * @param recordBuffer
     */
    virtual void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const;
    virtual void close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const;
    void setChild(ExecuteOperatorPtr child);
    virtual ~Operator();

  private:
    bool hasChildren() const;

  protected:
    mutable ExecuteOperatorPtr child;
};

}// namespace NES::Interpreter

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_OPERATOR_HPP_

#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_TRACE_CONSTANTVALUE_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_TRACE_CONSTANTVALUE_HPP_
#include <memory>
namespace NES::Interpreter {
class Any;
typedef std::shared_ptr<Any> AnyPtr;
class ConstantValue {
  public:
    ConstantValue(AnyPtr anyPtr);
    AnyPtr value;
    friend std::ostream& operator<<(std::ostream& os, const ConstantValue& tag);
};
}// namespace NES::Interpreter

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_TRACE_CONSTANTVALUE_HPP_

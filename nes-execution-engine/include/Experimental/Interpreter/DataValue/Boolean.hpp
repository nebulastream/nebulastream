#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_DATAVALUE_BOOLEAN_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_DATAVALUE_BOOLEAN_HPP_
#include <Experimental/Interpreter/DataValue/Any.hpp>
namespace NES::Interpreter {

class Boolean : public Any {
  public:
    const static Kind type = Kind::IntegerValue;

    Boolean(int64_t value) : Any(type), value(value){};
    Boolean(Boolean& a) : Boolean(a.value) {}

    std::unique_ptr<Any> copy() { return std::make_unique<Boolean>(this->value); }

    ~Boolean() {}

    bool getValue() { return value; }

    const bool value;
};

}// namespace NES::Interpreter

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_DATAVALUE_BOOLEAN_HPP_

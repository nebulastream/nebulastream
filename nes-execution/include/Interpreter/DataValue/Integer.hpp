#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_DATAVALUE_INTEGER_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_DATAVALUE_INTEGER_HPP_
#include <Interpreter/DataValue/Any.hpp>
#include <Interpreter/DataValue/Boolean.hpp>
namespace NES::Interpreter {

class Integer : public Any {
  public:
    const static Kind type = Kind::IntegerValue;

    Integer(int64_t value) : Any(type), value(value){};
    Integer(Integer& a) : Integer(a.value) {}

    std::unique_ptr<Any> copy() { return std::make_unique<Integer>(this->value); }

    std::unique_ptr<Integer> add(std::unique_ptr<Integer>& otherValue) const {
        return std::make_unique<Integer>(value + otherValue->value);
    };

    std::unique_ptr<Integer> sub(std::unique_ptr<Integer>& otherValue) const {
        return std::make_unique<Integer>(value - otherValue->value);
    };

    std::unique_ptr<Integer> mul(std::unique_ptr<Integer>& otherValue) const {
        return std::make_unique<Integer>(value * otherValue->value);
    };

    std::unique_ptr<Integer> div(std::unique_ptr<Integer>& otherValue) const {
        return std::make_unique<Integer>(value / otherValue->value);
    };

    std::unique_ptr<Boolean> equals(std::unique_ptr<Integer>& otherValue) const {
        return std::make_unique<Boolean>(value == otherValue->value);
    };

    std::unique_ptr<Boolean> lessThan(std::unique_ptr<Integer>& otherValue) const {
        return std::make_unique<Boolean>(value < otherValue->value);
    };

    ~Integer() {}

    int64_t getValue() { return value; }

    const int64_t value;
};

}// namespace NES::Interpreter

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_DATAVALUE_INTEGER_HPP_

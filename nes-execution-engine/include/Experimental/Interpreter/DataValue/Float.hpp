#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_DATAVALUE_FLOAT_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_DATAVALUE_FLOAT_HPP_
#include <Experimental/Interpreter/DataValue/Any.hpp>
namespace NES::Interpreter {
class Float : public Any {
  public:
    const static Kind type = FloatValue;

    Float(float value) : Any(type), value(value){};

    std::shared_ptr<Float> add(std::shared_ptr<Float>& otherValue) const { return create<Float>(value + otherValue->value); };

    std::unique_ptr<Any> copy() { return std::make_unique<Float>(this->value); }

    std::unique_ptr<Float> add(std::unique_ptr<Float>& otherValue) const {
        return std::make_unique<Float>(value + otherValue->value);
    };

    ~Float() {}

    const float value;
};
}// namespace NES::Interpreter

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_DATAVALUE_FLOAT_HPP_

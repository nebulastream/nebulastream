#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_DATAVALUE_ADDRESS_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_DATAVALUE_ADDRESS_HPP_
#include <Experimental/Interpreter/DataValue/Integer.hpp>
#include <Experimental/Interpreter/DataValue/Value.hpp>
namespace NES::Interpreter {
/*
class Address {
  public:
    Address(Value addressValue) : tc(tc), addressValue(std::move(addressValue)){};
    Value load() {
        auto intValue = std::make_unique<Integer>(10);
        Value result = Value(std::move(intValue), tc);
        tc->trace(LOAD, addressValue, result);
        return result;
    }

    void store(Value& value) { tc->trace(STORE, addressValue, value, value); };

    void* getPtr() {
        auto val = cast<Integer>(addressValue.value);
        return (void*) val->value;
    }

    Value& getAddress() { return addressValue; }

  private:
    TraceContext* tc;
    Value addressValue;
};
*/
}// namespace NES::Interpreter
#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_DATAVALUE_ADDRESS_HPP_

/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/
#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_DATAVALUE_ADDRESS_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_DATAVALUE_ADDRESS_HPP_
#include <Experimental/Interpreter/DataValue/Integer.hpp>
#include <Experimental/Interpreter/DataValue/Value.hpp>
namespace NES::Experimental::Interpreter {
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
}// namespace NES::Experimental::Interpreter
#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_DATAVALUE_ADDRESS_HPP_

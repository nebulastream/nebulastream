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
#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_DATAVALUE_INTEGER_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_DATAVALUE_INTEGER_HPP_
#include <Experimental/Interpreter/DataValue/Any.hpp>
#include <Experimental/Interpreter/DataValue/Boolean.hpp>
namespace NES::Experimental::Interpreter {

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

}// namespace NES::Experimental::Interpreter

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_DATAVALUE_INTEGER_HPP_

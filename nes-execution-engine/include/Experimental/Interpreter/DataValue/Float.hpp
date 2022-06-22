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
#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_DATAVALUE_FLOAT_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_DATAVALUE_FLOAT_HPP_
#include <Experimental/Interpreter/DataValue/Any.hpp>
namespace NES::ExecutionEngine::Experimental::Interpreter {
class Float : public Any {
  public:
    const static Kind type = FloatValue;

    Float(float value) : Any(type), value(value){};

    std::shared_ptr<Float> add(std::shared_ptr<Float>& otherValue) const { return create<Float>(value + otherValue->value); };

    std::unique_ptr<Any> copy() { return std::make_unique<Float>(this->value); }

    std::unique_ptr<Float> add(std::unique_ptr<Float>& otherValue) const {
        return std::make_unique<Float>(value + otherValue->value);
    };

    IR::Operations::Operation::BasicType getType() override { return IR::Operations::Operation::DOUBLE; }
    ~Float() {}

    const float value;
};
}// namespace NES::ExecutionEngine::Experimental::Interpreter

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_DATAVALUE_FLOAT_HPP_

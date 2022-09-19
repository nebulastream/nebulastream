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
#include "Experimental/NESIR/Types/StampFactory.hpp"
#include "Nautilus/Interface/DataValue/Any.hpp"
#include "Nautilus/Interface/DataValue/Boolean.hpp"
namespace NES::ExecutionEngine::Experimental::Interpreter {
class Float : public TraceableType {
  public:
    static const inline auto type = TypeIdentifier::create<Float>();

    Float(float value);
    IR::Types::StampPtr getType() const override;
    std::unique_ptr<Any> copy() override;
    std::unique_ptr<Float> add(const Float& otherValue) const;
    std::unique_ptr<Float> sub(const Float& otherValue) const;
    std::unique_ptr<Float> mul(const Float& otherValue) const;
    std::unique_ptr<Float> div(const Float& otherValue) const;
    std::unique_ptr<Boolean> equals(const Float& otherValue) const;
    std::unique_ptr<Boolean> lessThan(const Float& otherValue) const;
    std::unique_ptr<Boolean> greaterThan(const Float& otherValue) const;
    float getValue() const;
  private:
    const float value;
};
}// namespace NES::ExecutionEngine::Experimental::Interpreter

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_DATAVALUE_FLOAT_HPP_

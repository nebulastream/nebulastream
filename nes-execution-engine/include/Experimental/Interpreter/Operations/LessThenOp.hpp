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
#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATIONS_LESSTHENOP_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATIONS_LESSTHENOP_HPP_
#include <Experimental/Interpreter/DataValue/Any.hpp>
#include <Experimental/Interpreter/DataValue/Float.hpp>
#include <Experimental/Interpreter/DataValue/Integer.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::ExecutionEngine::Experimental::Interpreter::Operations {

template<typename Left, typename Right>
auto LessThan(const std::unique_ptr<Left>& leftExp, const std::unique_ptr<Right>& rightExp) {
    if (instanceOf<Integer>(leftExp) && instanceOf<Integer>(rightExp)) {
        auto leftValue = cast<Integer>(leftExp);
        auto rightValue = cast<Integer>(rightExp);
        return leftValue->lessThan(rightValue);
    }
    NES_THROW_RUNTIME_ERROR("no matching execution");
}

}// namespace NES::ExecutionEngine::Experimental::Interpreter::Operations

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATIONS_LESSTHENOP_HPP_

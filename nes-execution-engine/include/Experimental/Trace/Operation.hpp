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

#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_TRACE_OPERATION_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_TRACE_OPERATION_HPP_
#include <Experimental/Trace/BlockRef.hpp>
#include <Experimental/Trace/ConstantValue.hpp>
#include <Experimental/Trace/FunctionCallTarget.hpp>
#include <Experimental/Trace/OpCode.hpp>
#include <Experimental/Trace/ValueRef.hpp>
#include <variant>
#include <vector>
namespace NES::ExecutionEngine::Experimental::Trace {

using InputVariant = std::variant<ValueRef, ConstantValue, BlockRef, None, FunctionCallTarget>;
class OperationRef;
class Operation {
  public:
    Operation(OpCode op, std::vector<InputVariant> input) : op(op), result(None()), input(input){};
    Operation(OpCode op, ValueRef result, std::vector<InputVariant> input) : op(op), result(result), input(input){};
    Operation(OpCode op) : op(op), result(), input(){};
    Operation(const Operation& other)
        : op(other.op), result(other.result), input(other.input), operationRef(other.operationRef) {}
    ~Operation() {}
    OpCode op;
    InputVariant result;
    std::vector<InputVariant> input;
    std::shared_ptr<OperationRef> operationRef;
    friend std::ostream& operator<<(std::ostream& os, const Operation& operation);
};

}// namespace NES::ExecutionEngine::Experimental::Interpreter

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_TRACE_OPERATION_HPP_

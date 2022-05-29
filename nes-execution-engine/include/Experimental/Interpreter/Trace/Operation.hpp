//
// Created by pgrulich on 10.05.22.
//

#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_TRACE_OPERATION_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_TRACE_OPERATION_HPP_
#include <Experimental/Interpreter/Trace/BlockRef.hpp>
#include <Experimental/Interpreter/Trace/ConstantValue.hpp>
#include <Experimental/Interpreter/Trace/FunctionCallTarget.hpp>
#include <Experimental/Interpreter/Trace/OpCode.hpp>
#include <Experimental/Interpreter/Trace/ValueRef.hpp>
#include <variant>
#include <vector>
namespace NES::Interpreter {

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

}// namespace NES::Interpreter

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_TRACE_OPERATION_HPP_

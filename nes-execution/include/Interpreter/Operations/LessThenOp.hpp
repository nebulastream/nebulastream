#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATIONS_LESSTHENOP_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATIONS_LESSTHENOP_HPP_
#include <Interpreter/DataValue/Any.hpp>
#include <Interpreter/DataValue/Float.hpp>
#include <Interpreter/DataValue/Integer.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Interpreter::Operations {

std::unique_ptr<Any> LessThan(const std::unique_ptr<Any>& leftExp, const std::unique_ptr<Any>& rightExp) {
    if (instanceOf<Integer>(leftExp) && instanceOf<Integer>(rightExp)) {
        auto leftValue = cast<Integer>(leftExp);
        auto rightValue = cast<Integer>(rightExp);
        return leftValue->lessThan(rightValue);
    }
    NES_THROW_RUNTIME_ERROR("no matching execution");
}

}// namespace NES::Interpreter

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATIONS_LESSTHENOP_HPP_

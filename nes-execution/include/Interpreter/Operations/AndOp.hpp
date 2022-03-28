#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATIONS_ANDOP_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATIONS_ANDOP_HPP_
#include <Interpreter/DataValue/Any.hpp>
#include <Interpreter/DataValue/Float.hpp>
#include <Interpreter/DataValue/Integer.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Interpreter::Operations {

template<typename Left, typename Right>
auto AndOp(const std::unique_ptr<Left>& leftExp, const std::unique_ptr<Right>& rightExp) {
    if (instanceOf<Boolean>(leftExp) && instanceOf<Boolean>(rightExp)) {
        auto leftValue = cast<Boolean>(leftExp);
        auto rightValue = cast<Boolean>(rightExp);
        return std::make_unique<Boolean>(leftValue->value && rightValue->value);
    }
    NES_THROW_RUNTIME_ERROR("no matching execution");
}

}// namespace NES::Interpreter::Operations

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATIONS_ANDOP_HPP_

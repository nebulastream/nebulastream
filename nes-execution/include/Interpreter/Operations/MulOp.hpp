#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATIONS_MULOP_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATIONS_MULOP_HPP_
#include <Interpreter/DataValue/Any.hpp>
#include <Interpreter/DataValue/Float.hpp>
#include <Interpreter/DataValue/Integer.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Interpreter::Operations {

    template<typename Left, typename Right>
    auto MulOp(const std::unique_ptr<Left>& leftExp, const std::unique_ptr<Right>& rightExp) {
    if (instanceOf<Integer>(leftExp) && instanceOf<Integer>(rightExp)) {
        auto leftValue = cast<Integer>(leftExp);
        auto rightValue = cast<Integer>(rightExp);
        return leftValue->mul(rightValue);
    }
    NES_THROW_RUNTIME_ERROR("no matching execution");
}

}// namespace NES::Interpreter

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATIONS_MULOP_HPP_

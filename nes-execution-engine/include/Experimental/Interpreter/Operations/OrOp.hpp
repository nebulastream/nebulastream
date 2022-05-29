#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATIONS_OROP_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATIONS_OROP_HPP_
#include <Experimental/Interpreter/DataValue/Any.hpp>
#include <Experimental/Interpreter/DataValue/Float.hpp>
#include <Experimental/Interpreter/DataValue/Integer.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Interpreter::Operations {

template<typename Left, typename Right>
auto OrOp(const std::unique_ptr<Left>& leftExp, const std::unique_ptr<Right>& rightExp) {
    if (instanceOf<Boolean>(leftExp) && instanceOf<Boolean>(rightExp)) {
        auto leftValue = cast<Boolean>(leftExp);
        auto rightValue = cast<Boolean>(rightExp);
        return std::make_unique<Boolean>(leftValue->value || rightValue->value);
    }
    NES_THROW_RUNTIME_ERROR("no matching execution");
}

}// namespace NES::Interpreter::Operations

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATIONS_OROP_HPP_

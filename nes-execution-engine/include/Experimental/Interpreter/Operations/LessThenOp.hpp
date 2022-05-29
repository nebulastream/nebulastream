#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATIONS_LESSTHENOP_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATIONS_LESSTHENOP_HPP_
#include <Experimental/Interpreter/DataValue/Any.hpp>
#include <Experimental/Interpreter/DataValue/Float.hpp>
#include <Experimental/Interpreter/DataValue/Integer.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Interpreter::Operations {

template<typename Left, typename Right>
auto LessThan(const std::unique_ptr<Left>& leftExp, const std::unique_ptr<Right>& rightExp) {
    if (instanceOf<Integer>(leftExp) && instanceOf<Integer>(rightExp)) {
        auto leftValue = cast<Integer>(leftExp);
        auto rightValue = cast<Integer>(rightExp);
        return leftValue->lessThan(rightValue);
    }
    NES_THROW_RUNTIME_ERROR("no matching execution");
}

}// namespace NES::Interpreter::Operations

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATIONS_LESSTHENOP_HPP_

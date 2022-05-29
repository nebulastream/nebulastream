#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATIONS_SUBOP_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATIONS_SUBOP_HPP_
#include <Experimental/Interpreter/DataValue/Any.hpp>
#include <Experimental/Interpreter/DataValue/Float.hpp>
#include <Experimental/Interpreter/DataValue/Integer.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Interpreter::Operations {

template<typename Left, typename Right>
auto SubOp(const std::unique_ptr<Left>& leftExp, const std::unique_ptr<Right>& rightExp) {
    if (instanceOf<Integer>(leftExp) && instanceOf<Integer>(rightExp)) {
        auto leftValue = cast<Integer>(leftExp);
        auto rightValue = cast<Integer>(rightExp);
        return leftValue->sub(rightValue);
    }
    NES_THROW_RUNTIME_ERROR("no matching execution");
}



}// namespace NES::Interpreter

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATIONS_SUBOP_HPP_

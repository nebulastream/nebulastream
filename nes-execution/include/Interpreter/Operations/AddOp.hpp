#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATIONS_ADDOP_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATIONS_ADDOP_HPP_
#include <Interpreter/DataValue/Any.hpp>
#include <Interpreter/DataValue/MemRef.hpp>
#include <Interpreter/DataValue/Float.hpp>
#include <Interpreter/DataValue/Integer.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Interpreter::Operations {

template<typename Left, typename Right>
std::unique_ptr<Any> AddOp(const std::unique_ptr<Left>& leftExp, const std::unique_ptr<Right>& rightExp) {
    if (instanceOf<Integer>(leftExp) && instanceOf<Integer>(rightExp)) {
        auto leftValue = cast<Integer>(leftExp);
        auto rightValue = cast<Integer>(rightExp);
        return leftValue->add(rightValue);
    } else if (instanceOf<MemRef>(leftExp) && instanceOf<Integer>(rightExp)) {
        auto leftValue = cast<MemRef>(leftExp);
        std::unique_ptr<Integer> rightValue = cast<Integer>(rightExp);
        auto rightMemRef = std::make_unique<MemRef>(*rightValue);
        return leftValue->add(rightMemRef);
    }

    NES_THROW_RUNTIME_ERROR("no matching execution");
}

}// namespace NES::Interpreter::Operations

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATIONS_ADDOP_HPP_

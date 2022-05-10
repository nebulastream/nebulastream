#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATIONS_NEGATEOP_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATIONS_NEGATEOP_HPP_
#include <Interpreter/DataValue/Any.hpp>
#include <Interpreter/DataValue/Boolean.hpp>
#include <Interpreter/DataValue/Integer.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Interpreter::Operations {

template<typename Value>
auto NegateOp(const std::unique_ptr<Value>& value) {
    if (instanceOf<Boolean>(value)) {
        auto boolValue = cast<Boolean>(value);
        return std::make_unique<Boolean>(!boolValue->value);
    }
    NES_THROW_RUNTIME_ERROR("no matching execution");
}

}// namespace NES::Interpreter::Operations

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATIONS_NEGATEOP_HPP_

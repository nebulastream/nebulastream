#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATIONS_NEGATEOP_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATIONS_NEGATEOP_HPP_
#include <Interpreter/DataValue/Any.hpp>
#include <Interpreter/DataValue/Boolean.hpp>
#include <Interpreter/DataValue/Integer.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Interpreter::Operations {

std::unique_ptr<Any> NegateOp(const std::unique_ptr<Any>& value) {
    if (instanceOf<Boolean>(value)) {
        auto boolValue = cast<Boolean>(value);
        return std::make_unique<Boolean>(!boolValue->value);
    }
    NES_THROW_RUNTIME_ERROR("no matching execution");
}

}// namespace NES::Interpreter

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATIONS_NEGATEOP_HPP_

/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/
#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_DATAVALUE_VALUE_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_DATAVALUE_VALUE_HPP_
#include <Experimental/Interpreter/DataValue/Any.hpp>
#include <Experimental/Interpreter/DataValue/Boolean.hpp>
#include <Experimental/Interpreter/DataValue/Integer.hpp>
#include <Experimental/Interpreter/Operations/AddOp.hpp>
#include <Experimental/Interpreter/Operations/AndOp.hpp>
#include <Experimental/Interpreter/Operations/DivOp.hpp>
#include <Experimental/Interpreter/Operations/EqualsOp.hpp>
#include <Experimental/Interpreter/Operations/LessThenOp.hpp>
#include <Experimental/Interpreter/Operations/MulOp.hpp>
#include <Experimental/Interpreter/Operations/NegateOp.hpp>
#include <Experimental/Interpreter/Operations/OrOp.hpp>
#include <Experimental/Interpreter/Operations/SubOp.hpp>
#include <Experimental/Interpreter/Trace/OpCode.hpp>
#include <Experimental/Interpreter/Trace/TraceContext.hpp>
#include <Experimental/Interpreter/Trace/TraceManager.hpp>
#include <Experimental/Interpreter/Trace/ValueRef.hpp>
#include <Experimental/Interpreter/Util/Casting.hpp>
#include <Util/Logger/Logger.hpp>
#include <memory>
namespace NES::Experimental::Interpreter {

class BaseValue {
  public:
};

template<class ValueType = Any>
class Value : BaseValue {
  public:
    Value(std::unique_ptr<ValueType> wrappedValue) : value(std::move(wrappedValue)), ref(createNextRef()){};
    Value(std::unique_ptr<ValueType> wrappedValue, ValueRef& ref) : value(std::move(wrappedValue)), ref(ref){};

    Value(int value) : Value(std::make_unique<Integer>(value)) {
        Trace(CONST, *this, *this);
        //traceContext->trace(CONST, *this, *this);
    };

    explicit Value(int64_t value) : Value(std::make_unique<Integer>(value)) { Trace(CONST, *this, *this); };
    explicit Value(bool value) : Value(std::make_unique<Boolean>(value)) { Trace(CONST, *this, *this); };

    // copy constructor
    template<class OType>
    Value(const Value<OType>& other) : value(cast<ValueType>((other.value)->copy())), ref(other.ref) {}

    Value(const Value<ValueType>& other) : value(cast<ValueType>((other.value)->copy())), ref(other.ref) {}

    // move constructor
    template<class OType>
    Value(Value<OType>&& other) noexcept : value(std::exchange(other.value, nullptr)), ref(other.ref) {}

    // copy assignment
    Value& operator=(const Value& other) { return *this = Value(other); }

    // move assignment
    template<class OType>
    Value& operator=(Value<OType>&& other) noexcept {
        auto operation = Operation(ASSIGN, this->ref, {other.ref});
        getThreadLocalTraceContext()->trace(operation);
        this->value = cast<ValueType>(other.value);
        return *this;
    }

    operator bool() {
        //std::cout << "trace bool eval" << std::endl;
        if (instanceOf<Boolean>(value)) {
            auto boolValue = cast<Boolean>(value);
            Trace(OpCode::CMP, *this, *this);
            return boolValue->value;
        }
        return false;
    };

    template<class T>
    auto as() {
        return Value<T>(cast<T>(value), ref);
    }

    // destructor
    ~Value() {}

  public:
    std::unique_ptr<ValueType> value;
    ValueRef ref;
};

template<class ValueType, class ResultType>
void Trace(OpCode op, const Value<ValueType>& input, Value<ResultType>& result) {
    auto ctx = getThreadLocalTraceContext();
    if (ctx != nullptr) {
        if (op == OpCode::CONST) {
            auto constValue = input.value->copy();
            auto operation = Operation(op, result.ref, {ConstantValue(std::move(constValue))});
            ctx->trace(operation);
        } else if (op == CMP) {
            //if constexpr (std::is_same_v<ValueType, Any>)
            ctx->traceCMP(input.ref, cast<Boolean>(result.value)->value);
        } else {
            auto operation = Operation(op, result.ref, {input.ref});
            ctx->trace(operation);
        }
    }
};

template<class ValueLeft, class ValueRight, class ValueReturn>
void Trace(OpCode op, const Value<ValueLeft>& left, const Value<ValueRight>& right, Value<ValueReturn>& result) {
    auto ctx = getThreadLocalTraceContext();
    if (ctx != nullptr) {
        auto operation = Operation(op, result.ref, {left.ref, right.ref});
        ctx->trace(operation);
    }
};

template<class T>
concept IsValueType = std::is_base_of<BaseValue, T>::value;

template<class T>
concept IsNotValueType = !
std::is_base_of<BaseValue, T>::value;

template<class T>
    requires(std::is_same_v<T, const bool> == true)
inline auto toValue(T&& t) -> Value<> {
    auto value = Value<Boolean>(std::make_unique<Boolean>(t));
    Trace(CONST, value, value);
    return value;
}

template<class T>
    requires(std::is_same_v<T, const uint64_t> == true)
inline auto toValue(T&& t) -> Value<> {
    auto value = Value<Integer>(std::make_unique<Integer>(t));
    Trace(CONST, value, value);
    return value;
}

template<class T>
    requires(std::is_same_v<T, const int32_t> == true)
inline auto toValue(T&& t) -> Value<> {
    auto value = Value<Integer>(std::make_unique<Integer>(t));
    Trace(CONST, value, value);
    return value;
}

/*
template<class T>
    requires(std::is_fundamental<T>::value == true)
inline auto toValue(T&& t) -> Value<> {
    return Value{std::forward<T>(t)};
}
 */

template<IsNotValueType LHS, IsValueType RHS>
auto inline operator+(const LHS& left, const RHS& right) {
    auto leftValue = toValue(std::forward<const LHS>(left));
    return leftValue + right;
};

template<IsValueType LHS, IsNotValueType RHS>
auto inline operator+(const LHS& left, const RHS& right) {
    auto rightValue = toValue(std::forward<const RHS>(right));
    return left + rightValue;
};

template<IsValueType LHS, IsValueType RHS>
auto inline operator+(const LHS& left, const RHS& right) {
    auto result = Operations::AddOp(left.value, right.value);
    auto resValue = Value(std::move(result));
    Trace(OpCode::ADD, left, right, resValue);
    return resValue;
};

template<IsValueType LHS>
auto inline operator++(const LHS& left) {
    return left + (uint32_t) 1;
};

template<IsNotValueType LHS, IsValueType RHS>
auto inline operator-(const LHS& left, const RHS& right) {
    auto leftValue = toValue(std::forward<const LHS>(left));
    return leftValue - right;
};

template<IsValueType LHS, IsNotValueType RHS>
auto inline operator-(const LHS& left, const RHS& right) {
    auto rightValue = toValue(std::forward<const RHS>(right));
    return left - rightValue;
};

template<IsValueType LHS, IsValueType RHS>
auto inline operator-(const LHS& left, const RHS& right) {
    auto result = Operations::SubOp(left.value, right.value);
    auto resValue = Value(std::move(result));
    Trace(OpCode::SUB, left, right, resValue);
    return resValue;
};

template<IsNotValueType LHS, IsValueType RHS>
auto inline operator*(const LHS& left, const RHS& right) {
    auto leftValue = toValue(std::forward<const LHS>(left));
    return leftValue * right;
};

template<IsValueType LHS, IsNotValueType RHS>
auto inline operator*(const LHS& left, const RHS& right) {
    auto rightValue = toValue(std::forward<const RHS>(right));
    return left * rightValue;
};

template<IsValueType LHS, IsValueType RHS>
auto inline operator*(const LHS& left, const RHS& right) {
    auto result = Operations::MulOp(left.value, right.value);
    auto resValue = Value(std::move(result));
    Trace(OpCode::MUL, left, right, resValue);
    return resValue;
};

template<IsNotValueType LHS, IsValueType RHS>
auto inline operator/(const LHS& left, const RHS& right) {
    auto leftValue = toValue(std::forward<const LHS>(left));
    return leftValue / right;
};

template<IsValueType LHS, IsNotValueType RHS>
auto inline operator/(const LHS& left, const RHS& right) {
    auto rightValue = toValue(std::forward<const RHS>(right));
    return left / rightValue;
};

template<IsValueType LHS, IsValueType RHS>
auto inline operator/(const LHS& left, const RHS& right) {
    auto result = Operations::DivOp(left.value, right.value);
    auto resValue = Value(std::move(result));
    Trace(OpCode::DIV, left, right, resValue);
    return resValue;
};

// --- logical operations ----

template<IsNotValueType LHS, IsValueType RHS>
auto inline operator==(const LHS& left, const RHS& right) {
    auto leftValue = toValue(std::forward<const LHS>(left));
    return leftValue == right;
};

template<IsValueType LHS, IsNotValueType RHS>
auto inline operator==(const LHS& left, const RHS& right) {
    auto rightValue = toValue(std::forward<const RHS>(right));
    return left == rightValue;
};

template<IsValueType LHS, IsValueType RHS>
auto inline operator==(const LHS& left, const RHS& right) {
    auto result = Operations::EqualsOp(left.value, right.value);
    auto resValue = Value(std::move(result));
    Trace(OpCode::EQUALS, left, right, resValue);
    return resValue;
};

template<IsValueType LHS>
auto inline operator!(const LHS& left) {
    auto result = Operations::NegateOp(left.value);
    auto resValue = Value(std::move(result));
    Trace(OpCode::NEGATE, left, resValue);
    return resValue;
};

template<IsNotValueType LHS, IsValueType RHS>
auto inline operator!=(const LHS& left, const RHS& right) {
    auto leftValue = toValue(std::forward<const LHS>(left));
    return leftValue != right;
};

template<IsValueType LHS, IsNotValueType RHS>
auto inline operator!=(const LHS& left, const RHS& right) {
    auto rightValue = toValue(std::forward<const RHS>(right));
    return left != rightValue;
};

template<IsValueType LHS, IsValueType RHS>
auto inline operator!=(const LHS& left, const RHS& right) {
    return !(left == right);
};

template<IsNotValueType LHS, IsValueType RHS>
auto inline operator<(const LHS& left, const RHS& right) {
    auto leftValue = toValue(std::forward<const LHS>(left));
    return leftValue < right;
};

template<IsValueType LHS, IsNotValueType RHS>
auto inline operator<(const LHS& left, const RHS& right) {
    auto rightValue = toValue(std::forward<const RHS>(right));
    return left < rightValue;
};

template<IsValueType LHS, IsValueType RHS>
auto inline operator<(const LHS& left, const RHS& right) {
    auto result = Operations::LessThan(left.value, right.value);
    auto resValue = Value(std::move(result));
    Trace(OpCode::LESS_THAN, left, right, resValue);
    return resValue;
};

template<IsNotValueType LHS, IsValueType RHS>
auto inline operator>(const LHS& left, const RHS& right) {
    auto leftValue = toValue(std::forward<const LHS>(left));
    return leftValue > right;
};

template<IsValueType LHS, IsNotValueType RHS>
auto inline operator>(const LHS& left, const RHS& right) {
    auto rightValue = toValue(std::forward<const RHS>(right));
    return left > rightValue;
};

template<IsValueType LHS, IsValueType RHS>
auto inline operator>(const LHS& left, const RHS& right) {
    return !(left <= right);
};

template<IsNotValueType LHS, IsValueType RHS>
auto inline operator>=(const LHS& left, const RHS& right) {
    auto leftValue = toValue(std::forward<const LHS>(left));
    return leftValue >= right;
};

template<IsValueType LHS, IsNotValueType RHS>
auto inline operator>=(const LHS& left, const RHS& right) {
    auto rightValue = toValue(std::forward<const RHS>(right));
    return left >= rightValue;
};

template<IsValueType LHS, IsValueType RHS>
auto inline operator>=(const LHS& left, const RHS& right) {
    return !(left < right);
};

template<IsNotValueType LHS, IsValueType RHS>
auto inline operator<=(const LHS& left, const RHS& right) {
    auto leftValue = toValue(std::forward<const LHS>(left));
    return leftValue <= right;
};

template<IsValueType LHS, IsNotValueType RHS>
auto inline operator<=(const LHS& left, const RHS& right) {
    auto rightValue = toValue(std::forward<const RHS>(right));
    return left <= rightValue;
};

template<IsValueType LHS, IsValueType RHS>
auto inline operator<=(const LHS& left, const RHS& right) {
    return (left < right) || (left == right);
};

template<IsNotValueType LHS, IsValueType RHS>
auto inline operator&&(const LHS& left, const RHS& right) {
    auto leftValue = toValue(std::forward<const LHS>(left));
    return leftValue && right;
};

template<IsValueType LHS, IsNotValueType RHS>
auto inline operator&&(const LHS& left, const RHS& right) {
    auto rightValue = toValue(std::forward<const RHS>(right));
    return left && rightValue;
};

template<IsValueType LHS, IsValueType RHS>
auto inline operator&&(const LHS& left, const RHS& right) {
    auto result = Operations::AndOp(left.value, right.value);
    auto resValue = Value(std::move(result));
    Trace(OpCode::AND, left, right, resValue);
    return resValue;
};

template<IsNotValueType LHS, IsValueType RHS>
auto inline operator||(const LHS& left, const RHS& right) {
    auto leftValue = toValue(std::forward<const LHS>(left));
    return leftValue || right;
};

template<IsValueType LHS, IsNotValueType RHS>
auto inline operator||(const LHS& left, const RHS& right) {
    auto rightValue = toValue(std::forward<const RHS>(right));
    return left || rightValue;
};

template<IsValueType LHS, IsValueType RHS>
auto inline operator||(const LHS& left, const RHS& right) {
    auto result = Operations::OrOp(left.value, right.value);
    auto resValue = Value(std::move(result));
    Trace(OpCode::OR, left, right, resValue);
    return resValue;
};

template<class Type>
auto load(Value<MemRef>& ref) {
    auto value = ref.value->load<Type>();
    auto resValue = Value<Type>(std::move(value));
    Trace(OpCode::LOAD, ref, resValue);
    return resValue;
}

template<class Type>
void store(Value<MemRef>& ref, Value<Type>& value) {
    ref.value->store(value);
    if (auto* ctx = getThreadLocalTraceContext()) {
        auto operation = Operation(STORE, {ref.ref, value.ref});
        ctx->trace(operation);
    }
}

}// namespace NES::Experimental::Interpreter

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_DATAVALUE_VALUE_HPP_

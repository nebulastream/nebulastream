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
#include "Any.hpp"
#include "Boolean.hpp"
#include "Experimental/Interpreter/Util/Casting.hpp"
#include "Experimental/NESIR/Types/StampFactory.hpp"
#include "Experimental/Trace/ConstantValue.hpp"
#include "Experimental/Trace/OpCode.hpp"
#include "Experimental/Trace/SymbolicExecution/SymbolicExecutionContext.hpp"
#include "Experimental/Trace/TraceContext.hpp"
#include "Experimental/Trace/ValueRef.hpp"
#include "MemRef.hpp"
#include "Nautilus/Interface/DataValue/Float/Double.hpp"
#include "Nautilus/Interface/DataValue/Float/Float.hpp"
#include "Util/Logger/Logger.hpp"
#include <memory>
namespace NES::ExecutionEngine::Experimental::Interpreter {

template<class T>
Trace::ValueRef createNextRef(std::unique_ptr<T>& t) {
    auto ctx = Trace::getThreadLocalTraceContext();
    if (ctx) {
        return ctx->createNextRef(t->getType());
    }
    return Trace::ValueRef(0, 0, nullptr);
}

class BaseValue {};

template<class ValueType = Any>
class Value : BaseValue {
    using ValueTypePtr = std::shared_ptr<ValueType>;
  public:
    Value(ValueType wrappedValue) : value(std::make_unique<ValueType>(wrappedValue)), ref(createNextRef(value)){};
    Value(std::unique_ptr<ValueType> wrappedValue) : value(std::move(wrappedValue)), ref(createNextRef(value)){};
    Value(std::unique_ptr<ValueType> wrappedValue, Trace::ValueRef& ref) : value(std::move(wrappedValue)), ref(ref){};
    Value(int8_t value) : Value(std::make_unique<Int8>(value)) { TraceConstOperation(*this, *this); };
    Value(int16_t value) : Value(std::make_unique<Int16>(value)) { TraceConstOperation(*this, *this); };
    Value(int32_t value) : Value(std::make_unique<Int32>(value)) { TraceConstOperation(*this, *this); };
    Value(int64_t value) : Value(std::make_unique<Int64>(value)) { TraceConstOperation(*this, *this); };
    Value(uint8_t value) : Value(std::make_unique<UInt8>(value)) { TraceConstOperation(*this, *this); };
    Value(uint16_t value) : Value(std::make_unique<UInt16>(value)) { TraceConstOperation(*this, *this); };
    Value(uint32_t value) : Value(std::make_unique<UInt32>(value)) { TraceConstOperation(*this, *this); };
    Value(uint64_t value) : Value(std::make_unique<UInt64>(value)) { TraceConstOperation(*this, *this); };
    Value(float value) : Value(Any::create<Float>(value)) { TraceConstOperation(*this, *this); };
    Value(double value) : Value(Any::create<Double>(value)) { TraceConstOperation(*this, *this); };
    Value(int8_t* value) : Value(std::make_unique<MemRef>(value)) { TraceConstOperation(*this, *this); };
    Value(bool value) : Value(std::make_unique<Boolean>(value)) { TraceConstOperation(*this, *this); };

    // copy constructor
    //template<class OType>
    //Value(const Value<OType>& other) : value(cast<ValueType>((other.value)->copy())), ref(other.ref) {
    //    std::cout << "copy temp value" << std::endl;
    //}

    Value(const Value<ValueType>& other) : value(cast<ValueType>((other.value)->copy())), ref(other.ref) {}

    // move constructor
    template<class OType>
    Value(Value<OType>&& other) noexcept : value(std::exchange(other.value, nullptr)), ref(other.ref) {}

    // copy assignment
    Value& operator=(const Value& other) { return *this = Value<ValueType>(other); }

    // move assignment
    template<class OType>
    Value& operator=(Value<OType>&& other) noexcept {
        auto ctx = Trace::getThreadLocalTraceContext();
        if (ctx != nullptr) {
            auto operation = Trace::Operation(Trace::ASSIGN, this->ref, {other.ref});
            Trace::getThreadLocalTraceContext()->trace(operation);
        }
        this->value = cast<ValueType>(other.value);
        return *this;
    }

    operator bool() {
        if (isa<Boolean>(value)) {
            auto boolValue = cast<Boolean>(value);
            if (Trace::isInSymbolicExecution()) {
                auto* sec = Trace::getThreadLocalSymbolicExecutionContext();
                auto result = sec->executeCMP(this->ref);
                auto ctx = Trace::getThreadLocalTraceContext();
                ctx->traceCMP(ref, result);
                return result;
            } else {
                auto ctx = Trace::getThreadLocalTraceContext();
                if (ctx != nullptr) {
                    ctx->traceCMP(ref, boolValue->value);
                }
                return boolValue->value;
            }
        }
        return false;
        //NES_THROW_RUNTIME_ERROR("Can't evaluate bool on non Boolean value");
    };

    operator const Value<>() const {

        std::unique_ptr<Any> copy = value->copy();
        auto lRef = this->ref;
        return Value<>(std::move(copy), lRef);
    };

    template<typename ResultType, typename T = ValueType, typename = std::enable_if_t<std::is_same<T, MemRef>::value>>
    auto load() {
        std::unique_ptr<ResultType> result;
        if (Trace::isInSymbolicExecution()) {
            result = std::make_unique<ResultType>(0l);
        } else {
            result = ((MemRef*) this->value.get())->load<ResultType>();
        }
        auto resultValue = Value<ResultType>(std::move(result));
        TraceOperation(Trace::OpCode::LOAD, *this, resultValue);
        return resultValue;
    }

    template<typename InputValue, typename T = ValueType, typename = std::enable_if_t<std::is_same<T, MemRef>::value>>
    auto store(Value<InputValue>& storeValue) {
        if (!Trace::isInSymbolicExecution()) {
            this->value->store(storeValue.getValue());
        }
        if (auto* ctx = Trace::getThreadLocalTraceContext()) {
            auto operation = Trace::Operation(Trace::STORE, {ref, storeValue.ref});
            ctx->trace(operation);
        }
    }

    Value<> castTo(const TypeIdentifier* toStamp) const;

    template<class T>
    auto as() {
        return Value<T>(cast<T>(value), ref);
    }

    // destructor
    ~Value() {}

    ValueType* operator->() const { return value.get(); }

    ValueType& getValue() const { return *value.get(); };

  public:
    mutable std::unique_ptr<ValueType> value;
    Trace::ValueRef ref;
};

Value<> AddOp(const Value<>& leftExp, const Value<>& rightExp);
Value<> SubOp(const Value<>& leftExp, const Value<>& rightExp);
Value<> MulOp(const Value<>& leftExp, const Value<>& rightExp);
Value<> DivOp(const Value<>& leftExp, const Value<>& rightExp);
Value<> EqualsOp(const Value<>& leftExp, const Value<>& rightExp);
Value<> NegateOp(const Value<>& exp);
Value<> LessThanOp(const Value<>& leftExp, const Value<>& rightExp);
Value<> GreaterThanOp(const Value<>& leftExp, const Value<>& rightExp);
Value<> AndOp(const Value<>& leftExp, const Value<>& rightExp);
Value<> OrOp(const Value<>& leftExp, const Value<>& rightExp);

Value<> CastToOp(const Value<>& leftExp, IR::Types::StampPtr toStamp);

template<class ValueType, class ResultType>
void TraceConstOperation(const Value<ValueType>& input, Value<ResultType>& result) {
    auto ctx = Trace::getThreadLocalTraceContext();
    if (ctx != nullptr) {
        // fast case for expected operations
        if (ctx->isExpectedOperation(Trace::CONST)) {
            ctx->incrementOperationCounter();
            return;
        }
        auto constValue = input.value->copy();
        auto operation = Trace::Operation(Trace::CONST, result.ref, {Trace::ConstantValue(std::move(constValue))});
        ctx->trace(operation);
    }
};

template<class ValueType, class ResultType>
void TraceOperation(Trace::OpCode op, const Value<ValueType>& input, Value<ResultType>& result) {
    auto ctx = Trace::getThreadLocalTraceContext();
    if (ctx != nullptr) {
        if (op == Trace::OpCode::CONST) {
            // fast case for expected operations
            if (ctx->isExpectedOperation(op)) {
                ctx->incrementOperationCounter();
                return;
            }
            auto constValue = input.value->copy();
            auto operation = Trace::Operation(op, result.ref, {Trace::ConstantValue(std::move(constValue))});
            ctx->trace(operation);
        } else if (op == Trace::CMP) {
            //if constexpr (std::is_same_v<ValueType, Any>)
            ctx->traceCMP(input.ref, cast<Boolean>(result.value)->value);
        } else {
            // fast case for expected operations
            if (ctx->isExpectedOperation(op)) {
                ctx->incrementOperationCounter();
                return;
            }
            auto operation = Trace::Operation(op, result.ref, {input.ref});
            ctx->trace(operation);
        }
    }
};

template<class ValueLeft, class ValueRight, class ValueReturn>
void TraceOperation(Trace::OpCode op, const Value<ValueLeft>& left, const Value<ValueRight>& right, Value<ValueReturn>& result) {
    auto ctx = Trace::getThreadLocalTraceContext();
    if (ctx != nullptr) {
        // fast case for expected operations
        if (ctx->isExpectedOperation(op)) {
            ctx->incrementOperationCounter();
            return;
        }
        auto operation = Trace::Operation(op, result.ref, {left.ref, right.ref});
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
    TraceConstOperation(value, value);
    return value;
}

template<class T>
    requires(std::is_same_v<T, const float> == true)
inline auto toValue(T&& t) -> Value<> {
    return Value<Float>(t);
}

template<class T>
    requires(std::is_same_v<T, const int64_t> == true)
inline auto toValue(T&& t) -> Value<> {
    return Value<Int64>(t);
}

template<class T>
    requires(std::is_same_v<T, const uint64_t> == true)
inline auto toValue(T&& t) -> Value<> {
    return Value<UInt64>(t);
}

template<class T>
    requires(std::is_same_v<T, const int32_t> == true)
inline auto toValue(T&& t) -> Value<> {
    return Value<Int32>(t);
}

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
    return AddOp(left, right);
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
    return SubOp(left, right);
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
    return MulOp(left, right);
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
    return DivOp(left, right);
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
    return EqualsOp(left, right);
};

template<IsValueType LHS>
auto inline operator!(const LHS& left) {
    return NegateOp(left);
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
    return LessThanOp(left, right);
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
    return GreaterThanOp(left, right);
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
    return (left > right) || (left == right);
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
    auto result = AndOp(left, right);
    return result;
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
    return OrOp(left, right);
};

}// namespace NES::ExecutionEngine::Experimental::Interpreter

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_DATAVALUE_VALUE_HPP_

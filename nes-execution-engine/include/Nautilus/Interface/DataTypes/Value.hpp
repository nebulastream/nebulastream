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
#ifndef NES_NAUTILUS_INTERFACE_DATATYPES_VALUE_HPP_
#define NES_NAUTILUS_INTERFACE_DATATYPES_VALUE_HPP_
#include <Experimental/Interpreter/Util/Casting.hpp>
#include <Nautilus/IR/Types/StampFactory.hpp>
#include <Nautilus/Interface/DataTypes/Any.hpp>
#include <Nautilus/Interface/DataTypes/Boolean.hpp>
#include <Nautilus/Interface/DataTypes/Float/Double.hpp>
#include <Nautilus/Interface/DataTypes/Float/Float.hpp>
#include <Nautilus/Interface/DataTypes/MemRef.hpp>
#include <Nautilus/Tracing/SymbolicExecution/SymbolicExecutionContext.hpp>
#include <Nautilus/Tracing/Trace/ConstantValue.hpp>
#include <Nautilus/Tracing/Trace/OpCode.hpp>
#include <Nautilus/Tracing/TraceContext.hpp>
#include <Nautilus/Tracing/ValueRef.hpp>
#include <Util/Logger/Logger.hpp>
#include <memory>
namespace NES::ExecutionEngine::Experimental::Interpreter {

Nautilus::Tracing::ValueRef createNextValueReference(Nautilus::IR::Types::StampPtr&& stamp);
void TraceConstOperation(const Interpreter::AnyPtr& constValue, const Nautilus::Tracing::ValueRef& valueReference);
void traceAssignmentOperation(const Nautilus::Tracing::ValueRef& targetRef, const Nautilus::Tracing::ValueRef& sourceRef);
bool traceBoolOperation(const Interpreter::AnyPtr& boolValue, const Nautilus::Tracing::ValueRef& sourceRef);
void traceBinaryOperation(const Nautilus::Tracing::OpCode& op,
                          const Nautilus::Tracing::ValueRef& resultRef,
                          const Nautilus::Tracing::ValueRef& leftRef,
                          const Nautilus::Tracing::ValueRef& rightRef);
void traceUnaryOperation(const Nautilus::Tracing::OpCode& op, const Nautilus::Tracing::ValueRef& resultRef, const Nautilus::Tracing::ValueRef& inputRef);

class BaseValue {};

template<class T>
concept IsAnyType = std::is_base_of<Any, T>::value;

template<class T>
concept IsValueType = std::is_base_of<BaseValue, T>::value;

template<class T>
concept IsNotValueType = !
std::is_base_of<BaseValue, T>::value;

/**
 * @brief The Value class provides the elementary wrapper for any data value that inherents from Any.
 * Value provides operator overloading and integrates with the tracing framework to track individual operations, e.g. ==, +, -.
 * @tparam ValueType
 */
template<class ValueType = Any>
class Value : BaseValue {
    using ValueTypePtr = std::shared_ptr<ValueType>;

  public:
    /*
     * Creates a Value<Int8> object from an int8_t.
     */
    Value(int8_t value) : Value(std::make_shared<Int8>(value)) { TraceConstOperation(this->value, this->ref); };

    /*
     * Creates a Value<Int16> object from an int16_t.
     */
    Value(int16_t value) : Value(std::make_shared<Int16>(value)) { TraceConstOperation(this->value, this->ref); };

    /*
     * Creates a Value<Int32> object from an int32_t.
     */
    Value(int32_t value) : Value(std::make_shared<Int32>(value)) { TraceConstOperation(this->value, this->ref); };

    /*
     * Creates a Value<Int64> object from an int64_t.
     */
    Value(int64_t value) : Value(std::make_shared<Int64>(value)) { TraceConstOperation(this->value, this->ref); };

    /*
     * Creates a Value<UInt8> object from an uint8_t.
     */
    Value(uint8_t value) : Value(std::make_shared<UInt8>(value)) { TraceConstOperation(this->value, this->ref); };

    /*
     * Creates a Value<Int16> object from an int16_t.
     */
    Value(uint16_t value) : Value(std::make_shared<UInt16>(value)) { TraceConstOperation(this->value, this->ref); };

    /*
     * Creates a Value<Int32> object from an int32_t.
     */
    Value(uint32_t value) : Value(std::make_shared<UInt32>(value)) { TraceConstOperation(this->value, this->ref); };

    /*
     * Creates a Value<Int64> object from an int64_t.
     */
    Value(uint64_t value) : Value(std::make_shared<UInt64>(value)) { TraceConstOperation(this->value, this->ref); };

    /*
     * Creates a Value<Float> object from a float.
     */
    Value(float value) : Value(Any::create<Float>(value)) { TraceConstOperation(this->value, this->ref); };

    /*
     * Creates a Value<Double> object from a double.
     */
    Value(double value) : Value(Any::create<Double>(value)) { TraceConstOperation(this->value, this->ref); };

    /*
     * Creates a Value<Boolean> object from a bool.
     */
    Value(bool value) : Value(std::make_shared<Boolean>(value)) { TraceConstOperation(this->value, this->ref); };

    /*
     * Creates a Value<MemRef> object from an int8_t*.
     */
    Value(int8_t* value) : Value(std::make_shared<MemRef>(value)) { TraceConstOperation(this->value, this->ref); };

    /**
     * @brief copy constructor
     */
    Value(std::shared_ptr<ValueType> wrappedValue)
        : value(std::move(wrappedValue)), ref(createNextValueReference(value->getType())){};

    /**
     * @brief copy constructor
     */
    template<IsAnyType OType>
    Value(OType wrappedValue) : value(std::make_shared<OType>(wrappedValue)), ref(createNextValueReference(value->getType())){};

    /**
     * @brief copy constructor
     */
    Value(std::shared_ptr<ValueType> wrappedValue, Nautilus::Tracing::ValueRef& ref) : value(std::move(wrappedValue)), ref(ref){};

    /**
     * @brief copy constructor
     */
    Value(const Value<ValueType>& other) : value(cast<ValueType>((other.value)->copy())), ref(other.ref) {}

    /**
     * @brief move constructor
     */
    template<class OType>
    Value(Value<OType>&& other) noexcept : value(std::exchange(other.value, nullptr)), ref(other.ref) {}

    /**
     * @brief copy assignment assignment
     * @param other value
     * @return value
     */
    Value& operator=(const Value& other) { return *this = Value<ValueType>(other); }

    /**
     * @brief move assignment constructor
     * @tparam OType value type
     * @param other value
     * @return value
     */
    template<class OType>
    Value& operator=(Value<OType>&& other) noexcept {
        traceAssignmentOperation(this->ref, other.ref);
        this->value = cast<ValueType>(other.value);
        return *this;
    }

    operator const Value<>() const {
        std::shared_ptr<Any> copy = value->copy();
        auto lRef = this->ref;
        return Value<>(std::move(copy), lRef);
    };

    /**
     * @brief Special case flor loads on memref values.
     * @tparam ResultType
     * @tparam T
     * @return
     */
    template<typename ResultType, typename T = ValueType, typename = std::enable_if_t<std::is_same<T, MemRef>::value>>
    auto load() {
        std::shared_ptr<ResultType> result;
        if (Nautilus::Tracing::isInSymbolicExecution()) {
            result = std::make_shared<ResultType>(0l);
        } else {
            result = ((MemRef*) this->value.get())->load<ResultType>();
        }
        auto resultValue = Value<ResultType>(std::move(result));
        traceUnaryOperation(Nautilus::Tracing::OpCode::LOAD, resultValue.ref, this->ref);
        return resultValue;
    }

    /**
     * @brief Special case flor stores on memref values.
     * @tparam ResultType
     * @tparam T
     * @return void
     */
    template<typename InputValue, typename T = ValueType, typename = std::enable_if_t<std::is_same<T, MemRef>::value>>
    void store(Value<InputValue>& storeValue) {
        if (!Nautilus::Tracing::isInSymbolicExecution()) {
            this->value->store(storeValue.getValue());
        }
        if (auto* ctx = Nautilus::Tracing::getThreadLocalTraceContext()) {
            auto operation = Nautilus::Tracing::TraceOperation(Nautilus::Tracing::STORE, {ref, storeValue.ref});
            ctx->trace(operation);
        }
    }

    Value<> castTo(const TypeIdentifier* toStamp) const;

    template<class T>
    auto as() {
        return Value<T>(cast<T>(value), ref);
    }

    /**
     * @brief Bool operator overload.
     * The result of this method is determined by the tracing component.
     * Symbolic tracing uses this method to manipulate the path through a function.
     * Otherwise returns the current boolean value if it is a boolean
     * @return bool.
     */
    operator bool() { return traceBoolOperation(value, ref); };
    ValueType* operator->() const { return value.get(); }
    ValueType& getValue() const { return *value.get(); };

    /**
     * @brief destructor
     */
    ~Value() {}

  public:
    mutable ValueTypePtr value;
    Nautilus::Tracing::ValueRef ref;
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
Value<> CastToOp(const Value<>& leftExp, Nautilus::IR::Types::StampPtr toStamp);

template<class T>
    requires(std::is_same_v<T, const bool> == true)
inline auto toValue(T&& t) -> Value<> {
    auto value = Value<Boolean>(std::make_unique<Boolean>(t));
    TraceConstOperation(value.value, value.ref);
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

template<class T>
    requires(std::is_same_v<T, const int8_t> == true)
inline auto toValue(T&& t) -> Value<> {
    return Value<Int8>(t);
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

#endif//NES_NAUTILUS_INTERFACE_DATATYPES_VALUE_HPP_

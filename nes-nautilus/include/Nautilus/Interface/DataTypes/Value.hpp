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
#ifndef NES_NAUTILUS_INCLUDE_NAUTILUS_INTERFACE_DATATYPES_VALUE_HPP_
#define NES_NAUTILUS_INCLUDE_NAUTILUS_INTERFACE_DATATYPES_VALUE_HPP_
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/IR/Types/StampFactory.hpp>
#include <Nautilus/Interface/DataTypes/Any.hpp>
#include <Nautilus/Interface/DataTypes/BaseTypedRef.hpp>
#include <Nautilus/Interface/DataTypes/Boolean.hpp>
#include <Nautilus/Interface/DataTypes/Float/Double.hpp>
#include <Nautilus/Interface/DataTypes/Float/Float.hpp>
#include <Nautilus/Interface/DataTypes/Identifier.hpp>
#include <Nautilus/Interface/DataTypes/MemRef.hpp>
#include <Nautilus/Tracing/TraceUtil.hpp>
#include <Nautilus/Tracing/ValueRef.hpp>
#include <Nautilus/Util/Casting.hpp>
#include <cstdint>
#include <memory>
#include <type_traits>
#include <utility>
namespace NES::Nautilus {

Tracing::ValueRef createNextValueReference(IR::Types::StampPtr&& stamp);

class BaseValue {};

template<class T>
concept IsAnyType = std::is_base_of<Any, T>::value;

template<class T>
concept IsValueType = std::is_base_of<BaseValue, T>::value;

template<class T>
concept IsNotValueType = !std::is_base_of<BaseValue, T>::value;

template<typename P>
struct ValueForPrimitive;

template<>
struct ValueForPrimitive<int8_t> {
    using type = Int8;
};

template<>
struct ValueForPrimitive<int16_t> {
    using type = Int16;
};

template<>
struct ValueForPrimitive<int32_t> {
    using type = Int32;
};

template<>
struct ValueForPrimitive<int64_t> {
    using type = Int64;
};

template<>
struct ValueForPrimitive<uint8_t> {
    using type = UInt8;
};

template<>
struct ValueForPrimitive<uint16_t> {
    using type = UInt16;
};

template<>
struct ValueForPrimitive<uint32_t> {
    using type = UInt32;
};

template<>
struct ValueForPrimitive<uint64_t> {
    using type = UInt64;
};

template<>
struct ValueForPrimitive<float> {
    using type = Float;
};

template<>
struct ValueForPrimitive<double> {
    using type = Double;
};

/**
 * @brief The Value class provides the elementary wrapper for any data value that inherits from Any.
 * Value provides operator overloading and integrates with the tracing framework to track individual operations, e.g. ==, +, -.
 * @tparam ValueType
 */
template<class ValueType = Any>
class Value : BaseValue {
    using ValueTypePtr = std::shared_ptr<ValueType>;

  public:
    class ValueIndexReference {
      public:
        ValueIndexReference(Value<> index, Value<ValueType>& inputValue)
            : index(std::make_unique<Value<>>(index)), inputValue(inputValue){};
        operator const Value<>() const;
        ValueIndexReference& operator=(const Value<>&);

      private:
        std::unique_ptr<Value<>> index;
        Value<ValueType>& inputValue;
    };
    using element_type = ValueType;

    /*
     * Creates a Value<Int8> object from a std::int8_t.
     */
    Value(int8_t value) : Value(std::make_shared<Int8>(value)) {
        Tracing::TraceUtil::traceConstOperation(this->value, this->ref);
    };

    /*
     * Creates a Value<UInt8> object from an std::uint8_t.
     */
    Value(uint8_t value) : Value(std::make_shared<UInt8>(value)) {
        Tracing::TraceUtil::traceConstOperation(this->value, this->ref);
    };

    /*
     * Creates a Value<Int16> object from an std::int16_t.
     */
    Value(int16_t value) : Value(std::make_shared<Int16>(value)) {
        Tracing::TraceUtil::traceConstOperation(this->value, this->ref);
    };

    /*
     * Creates a Value<UInt16> object from an std::uint16_t.
     */
    Value(uint16_t value) : Value(std::make_shared<UInt16>(value)) {
        Tracing::TraceUtil::traceConstOperation(this->value, this->ref);
    };

    /*
     * Creates a Value<Int32> object from an std::int32_t.
     */
    Value(int32_t value) : Value(std::make_shared<Int32>(value)) {
        Tracing::TraceUtil::traceConstOperation(this->value, this->ref);
    };

    /*
     * Creates a Value<UInt32> object from an std::uint32_t.
     */
    Value(uint32_t value) : Value(std::make_shared<UInt32>(value)) {
        Tracing::TraceUtil::traceConstOperation(this->value, this->ref);
    };

    /*
     * Creates a Value<Int64> object from an std::int64_t.
     */
    Value(int64_t value) : Value(std::make_shared<Int64>(value)) {
        Tracing::TraceUtil::traceConstOperation(this->value, this->ref);
    };

    /*
     * Creates a Value<UInt64> object from an std::uint64_t.
     */
    Value(uint64_t value) : Value(std::make_shared<UInt64>(value)) {
        Tracing::TraceUtil::traceConstOperation(this->value, this->ref);
    };

    /*
     * Creates a Value<Float> object from a float.
     */
    Value(float value) : Value(Any::create<Float>(value)) { Tracing::TraceUtil::traceConstOperation(this->value, this->ref); };

    /*
     * Creates a Value<Double> object from a double.
     */
    Value(double value) : Value(Any::create<Double>(value)) { Tracing::TraceUtil::traceConstOperation(this->value, this->ref); };

    template<typename T = ValueType, typename = std::enable_if_t<std::negation<std::is_same<T, MemRef>>::value>>
    Value(const char* value);

    /*
     * Creates a Value<Boolean> object from a bool.
     */
    Value(bool value) : Value(std::make_shared<Boolean>(value)) {
        Tracing::TraceUtil::traceConstOperation(this->value, this->ref);
    };

    /*
     * Creates a Value<MemRef> object from an std::int8_t*.
     */
    template<typename T = ValueType>
        requires(std::is_same_v<T, MemRef>)
    Value(std::int8_t* value) : Value(std::make_shared<MemRef>(value)) {
        Tracing::TraceUtil::traceConstOperation(this->value, this->ref);
    };

    template<NESIdentifier T = ValueType>
    explicit Value(T identifier) : Value(std::static_pointer_cast<const Any>(std::make_shared<Identifier<T>>(identifier))) {
        Tracing::TraceUtil::traceConstOperation(this->value, this->ref);
    }

    Value(ValueType&& value) : value(std::make_shared<ValueType>(value)), ref(createNextValueReference(value.getType())){};

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
    Value(const Value<ValueType>& other) : value(cast<ValueType>((other.value))), ref(other.ref) {}

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
        Tracing::TraceUtil::traceAssignmentOperation(this->ref, other.ref);
        this->value = cast<ValueType>(other.value);
        return *this;
    }

    operator const Value<>() const { return Value<>(value, ref); };

    /**
     * @brief Special case flor loads on memref values.
     * @tparam ResultType
     * @tparam T
     * @return
     */
    template<typename ResultType, typename T = ValueType, typename = std::enable_if_t<std::is_same<T, MemRef>::value>>
    auto load() {
        if (Tracing::TraceUtil::inTracer()) {
            if constexpr (std::is_same_v<ResultType, MemRef>) {
                auto result = std::make_shared<MemRef>(nullptr);
                auto resultValue = Value<ResultType>(std::move(result));
                Tracing::TraceUtil::traceUnaryOperation(Nautilus::Tracing::OpCode::LOAD, resultValue.ref, this->ref);
                return resultValue;
            } else {
                auto result = std::make_shared<ResultType>(0);
                auto resultValue = Value<ResultType>(std::move(result));
                Tracing::TraceUtil::traceUnaryOperation(Nautilus::Tracing::OpCode::LOAD, resultValue.ref, this->ref);
                return resultValue;
            }
        } else {
            auto result = ((MemRef*) this->value.get())->load<ResultType>();
            return Value<ResultType>(std::move(result));
        }
    }

    /**
     * @brief Special case flor stores on memref values.
     * @tparam ResultType
     * @tparam T
     * @return void
     */
    template<typename InputValue, typename T = ValueType, typename = std::enable_if_t<std::is_same<T, MemRef>::value>>
    void store(const Value<InputValue>& storeValue) {
        if (Tracing::TraceUtil::inInterpreter()) {
            this->value->store(storeValue.getValue());
        } else {
            Tracing::TraceUtil::traceStoreOperation(ref, storeValue.ref);
        }
    }

    ValueIndexReference inline operator[](uint32_t index);
    template<class T>
    ValueIndexReference inline operator[](Value<T>& index) {
        return ValueIndexReference(index, *this);
    };

    Value<> castTo(const TypeIdentifier* toStamp) const;

    template<class T>
    auto as() const {
        return Value<T>(cast<T>(value), ref);
    }

    /**
     * @brief Bool operator overload.
     * The result of this method is determined by the tracing component.
     * Symbolic tracing uses this method to manipulate the path through a function.
     * Otherwise returns the current boolean value if it is a boolean
     * @return bool.
     */
    operator bool() { return Tracing::TraceUtil::traceBoolOperation(value, ref); };
    ValueType* operator->() const { return value.get(); }
    ValueType& getValue() const { return *value.get(); };

    bool isTracableType() const { return std::dynamic_pointer_cast<TraceableType>(value) != nullptr; }

  public:
    mutable ValueTypePtr value;
    mutable Nautilus::Tracing::ValueRef ref;
};

Value<> AddOp(const Value<>& leftExp, const Value<>& rightExp);
Value<> SubOp(const Value<>& leftExp, const Value<>& rightExp);
Value<> MulOp(const Value<>& leftExp, const Value<>& rightExp);
Value<> DivOp(const Value<>& leftExp, const Value<>& rightExp);
Value<> ModOp(const Value<>& leftExp, const Value<>& rightExp);
Value<> EqualsOp(const Value<>& leftExp, const Value<>& rightExp);
Value<> NegateOp(const Value<>& exp);
Value<> LessThanOp(const Value<>& leftExp, const Value<>& rightExp);
Value<> GreaterThanOp(const Value<>& leftExp, const Value<>& rightExp);
Value<> AndOp(const Value<>& leftExp, const Value<>& rightExp);
Value<> OrOp(const Value<>& leftExp, const Value<>& rightExp);
Value<> BitWiseAndOp(const Value<>& leftExp, const Value<>& rightExp);
Value<> BitWiseOrOp(const Value<>& leftExp, const Value<>& rightExp);
Value<> BitWiseXorOp(const Value<>& leftExp, const Value<>& rightExp);
Value<> BitWiseLeftShiftOp(const Value<>& leftExp, const Value<>& rightExp);
Value<> BitWiseRightShiftOp(const Value<>& leftExp, const Value<>& rightExp);
Value<> CastToOp(const Value<>& leftExp, Nautilus::IR::Types::StampPtr toStamp);
Value<> ReadArrayIndexOp(const Value<>& input, Value<UInt32>& index);
void WriteArrayIndexOp(const Value<>& input, Value<UInt32>& index, const Value<>& value);

template<typename LHS>
Value<LHS>::ValueIndexReference::operator const Value<>() const {
    auto idx = index->as<UInt32>();
    return ReadArrayIndexOp(inputValue, idx);
}

template<typename LHS>
typename Value<LHS>::ValueIndexReference& Value<LHS>::ValueIndexReference::operator=(const Value<>& value) {
    auto idx = index->as<UInt32>();
    WriteArrayIndexOp(inputValue, idx, value);
    return *this;
}

template<typename T, typename = std::enable_if_t<std::is_constructible_v<Value<>, std::decay_t<T>>>>
inline auto toValue(T&& t) {
    return Value<>(std::forward<T>(t));
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
    return left + (std::uint32_t) 1;
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

template<IsNotValueType LHS, IsValueType RHS>
auto inline operator%(const LHS& left, const RHS& right) {
    auto leftValue = toValue(std::forward<const LHS>(left));
    return leftValue % right;
};

template<IsValueType LHS, IsNotValueType RHS>
auto inline operator%(const LHS& left, const RHS& right) {
    auto rightValue = toValue(std::forward<const RHS>(right));
    return left % rightValue;
};

template<IsValueType LHS, IsValueType RHS>
auto inline operator%(const LHS& left, const RHS& right) {
    return ModOp(left, right);
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

template<IsNotValueType LHS, IsValueType RHS>
auto inline operator&(const LHS& left, const RHS& right) {
    auto leftValue = toValue(std::forward<const LHS>(left));
    return leftValue & right;
};

template<IsValueType LHS, IsNotValueType RHS>
auto inline operator&(const LHS& left, const RHS& right) {
    auto rightValue = toValue(std::forward<const RHS>(right));
    return left & rightValue;
};

template<IsValueType LHS, IsValueType RHS>
auto inline operator&(const LHS& left, const RHS& right) {
    return BitWiseAndOp(left, right);
};

template<IsNotValueType LHS, IsValueType RHS>
auto inline operator|(const LHS& left, const RHS& right) {
    auto leftValue = toValue(std::forward<const LHS>(left));
    return leftValue | right;
};

template<IsValueType LHS, IsNotValueType RHS>
auto inline operator|(const LHS& left, const RHS& right) {
    auto rightValue = toValue(std::forward<const RHS>(right));
    return left | rightValue;
};

template<IsValueType LHS, IsValueType RHS>
auto inline operator|(const LHS& left, const RHS& right) {
    return BitWiseOrOp(left, right);
};

template<IsNotValueType LHS, IsValueType RHS>
auto inline operator^(const LHS& left, const RHS& right) {
    auto leftValue = toValue(std::forward<const LHS>(left));
    return leftValue ^ right;
};

template<IsValueType LHS, IsNotValueType RHS>
auto inline operator^(const LHS& left, const RHS& right) {
    auto rightValue = toValue(std::forward<const RHS>(right));
    return left ^ rightValue;
};

template<IsValueType LHS, IsValueType RHS>
auto inline operator^(const LHS& left, const RHS& right) {
    return BitWiseXorOp(left, right);
};

template<IsNotValueType LHS, IsValueType RHS>
auto inline operator<<(const LHS& left, const RHS& right) {
    auto leftValue = toValue(std::forward<const LHS>(left));
    return leftValue << right;
};

template<IsValueType LHS, IsNotValueType RHS>
auto inline operator<<(const LHS& left, const RHS& right) {
    auto rightValue = toValue(std::forward<const RHS>(right));
    return left << rightValue;
};

template<IsValueType LHS, IsValueType RHS>
auto inline operator<<(const LHS& left, const RHS& right) {
    return BitWiseLeftShiftOp(left, right);
};

template<IsNotValueType LHS, IsValueType RHS>
auto inline operator>>(const LHS& left, const RHS& right) {
    auto leftValue = toValue(std::forward<const LHS>(left));
    return leftValue >> right;
};

template<IsValueType LHS, IsNotValueType RHS>
auto inline operator>>(const LHS& left, const RHS& right) {
    auto rightValue = toValue(std::forward<const RHS>(right));
    return left >> rightValue;
};

template<IsValueType LHS, IsValueType RHS>
auto inline operator>>(const LHS& left, const RHS& right) {
    return BitWiseRightShiftOp(left, right);
};

template<IsValueType LHS>
auto inline operator~(const LHS& left) {
    return BitWiseXorOp(left, -1);
};

template<typename T>
std::ostream& operator<<(std::ostream& out, Value<T>& value) {
    return out << value.getValue().toString();
}

template<typename T>
void PrintTo(const Value<T>& value, std::ostream* os) {
    *os << value.getValue().toString();
}

template<class ValueType>
typename Value<ValueType>::ValueIndexReference Value<ValueType>::operator[](uint32_t index) {
    auto indexValue = toValue(index);
    return ValueIndexReference(indexValue, *this);
}

}// namespace NES::Nautilus

#endif// NES_NAUTILUS_INCLUDE_NAUTILUS_INTERFACE_DATATYPES_VALUE_HPP_

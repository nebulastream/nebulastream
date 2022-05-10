#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_DATAVALUE_VALUE_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_DATAVALUE_VALUE_HPP_
#include <Interpreter/DataValue/Any.hpp>
#include <Interpreter/DataValue/Boolean.hpp>
#include <Interpreter/DataValue/Integer.hpp>
#include <Interpreter/Operations/AddOp.hpp>
#include <Interpreter/Operations/AndOp.hpp>
#include <Interpreter/Operations/DivOp.hpp>
#include <Interpreter/Operations/EqualsOp.hpp>
#include <Interpreter/Operations/LessThenOp.hpp>
#include <Interpreter/Operations/MulOp.hpp>
#include <Interpreter/Operations/NegateOp.hpp>
#include <Interpreter/Operations/OrOp.hpp>
#include <Interpreter/Operations/SubOp.hpp>
#include <Interpreter/Trace/OpCode.hpp>
#include <Interpreter/Trace/TraceContext.hpp>
#include <Interpreter/Trace/TraceManager.hpp>
#include <Interpreter/Trace/ValueRef.hpp>
#include <Interpreter/Util/Casting.hpp>
#include <Util/Logger/Logger.hpp>
#include <memory>
namespace NES::Interpreter {

class BaseValue {
  public:
};

template<class ValueType = Any>
class Value : BaseValue {
  public:
    Value(std::unique_ptr<ValueType> wrappedValue) : value(std::move(wrappedValue)), ref(createNextRef()){};

    Value(int value) : Value(std::make_unique<Integer>(value)) {
        Trace(CONST, *this, *this);
        //traceContext->trace(CONST, *this, *this);
    };

    Value(int64_t value) : Value(std::make_unique<Integer>(value)) { Trace(CONST, *this, *this); };
    Value(bool value) : Value(std::make_unique<Boolean>(value)) { Trace(CONST, *this, *this); };

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
        return Value<T>(cast<T>(value));
    }

    // destructor
    ~Value() {}

  public:
    std::unique_ptr<ValueType> value;
    ValueRef ref;
};

template<class ValueType>
void Trace(OpCode op, const Value<ValueType>& input, Value<ValueType>& result) {
    auto ctx = getThreadLocalTraceContext();
    if (ctx != nullptr) {
        if (op == OpCode::CONST) {
            auto constValue = input.value->copy();
            auto operation = Operation(op, result.ref, {ConstantValue(std::move(constValue))});
            ctx->trace(operation);
        } else if (op == CMP) {
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
    requires(std::is_fundamental<T>::value == true)
inline auto toValue(T&& t) -> Value<> {
    return Value{std::forward<T>(t)};
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
    auto result = Operations::AddOp(left.value, right.value);
    auto resValue = Value(std::move(result));
    Trace(OpCode::ADD, left, right, resValue);
    return resValue;
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

}// namespace NES::Interpreter

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_DATAVALUE_VALUE_HPP_

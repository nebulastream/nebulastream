#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_DATAVALUE_VALUE_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_DATAVALUE_VALUE_HPP_
#include <Interpreter/DataValue/Any.hpp>
#include <Interpreter/DataValue/Boolean.hpp>
#include <Interpreter/DataValue/Integer.hpp>
#include <Interpreter/Tracer.hpp>
#include <Interpreter/Util/Casting.hpp>
#include <Util/Logger/Logger.hpp>

#include <memory>
namespace NES::Interpreter {

namespace Operations {
std::unique_ptr<Any> AddOp(const std::unique_ptr<Any>& leftExp, const std::unique_ptr<Any>& rightExp);
std::unique_ptr<Any> AndOp(const std::unique_ptr<Any>& leftExp, const std::unique_ptr<Any>& rightExp);
std::unique_ptr<Any> DivOp(const std::unique_ptr<Any>& leftExp, const std::unique_ptr<Any>& rightExp);
std::unique_ptr<Any> EqualsOp(const std::unique_ptr<Any>& leftExp, const std::unique_ptr<Any>& rightExp);
std::unique_ptr<Any> LessThan(const std::unique_ptr<Any>& leftExp, const std::unique_ptr<Any>& rightExp);
std::unique_ptr<Any> MulOp(const std::unique_ptr<Any>& leftExp, const std::unique_ptr<Any>& rightExp);
std::unique_ptr<Any> OrOp(const std::unique_ptr<Any>& leftExp, const std::unique_ptr<Any>& rightExp);
std::unique_ptr<Any> SubOp(const std::unique_ptr<Any>& leftExp, const std::unique_ptr<Any>& rightExp);
std::unique_ptr<Any> NegateOp(const std::unique_ptr<Any>& leftExp);
}// namespace Operations

class Value {
  public:
    //Value(std::unique_ptr<Any>& wrappedValue, TraceContext* traceContext)
    //    : value(std::move(wrappedValue)), traceContext(traceContext), ref(traceContext->createNextRef()){};
    Value(std::unique_ptr<Any> wrappedValue, TraceContext* traceContext)
        : value(std::move(wrappedValue)), traceContext(traceContext), ref(traceContext->createNextRef()), srcRef(ref){

                                                                                                          };

    Value(std::unique_ptr<Any>& wrappedValue) : value(std::move(wrappedValue)), ref(0, 0), srcRef(ref){};
    Value(int value, TraceContext* traceContext) : Value(std::make_unique<Integer>(value), traceContext) {
        traceContext->trace(CONST, *this, *this);
    };
    Value(bool value, TraceContext* traceContext) : Value(std::make_unique<Boolean>(value), traceContext) {
        traceContext->trace(CONST, *this, *this);
    };
    Value(int64_t value, TraceContext* traceContext) : Value(std::make_unique<Integer>(value), traceContext) {
        traceContext->trace(CONST, *this, *this);
    };

    // copy constructor
    Value(const Value& other) : value(other.value->copy()), traceContext(other.traceContext), ref(other.ref), srcRef(other.ref) {}

    // move constructor
    Value(Value&& other) noexcept
        : value(std::exchange(other.value, nullptr)), traceContext(other.traceContext), ref(other.ref), srcRef(other.ref) {}

    // copy assignment
    Value& operator=(const Value& other) { return *this = Value(other); }

    // move assignment
    Value& operator=(Value&& other) noexcept {
        if (traceContext) {
            Trace(traceContext, OpCode::ASSIGN, other, *this);
            //traceContext->getExecutionTrace().traceAssignment(srcRef, other.ref);
        }
        std::swap(value, other.value);
        //std::swap(ref, other.ref);
        return *this;
    }

    operator bool() {
        //std::cout << "trace bool eval" << std::endl;
        if (instanceOf<Boolean>(value)) {
            auto boolValue = cast<Boolean>(value);
            Trace(traceContext, OpCode::CMP, *this, *this);
            return boolValue->value;
        }
        return false;
    };

    // destructor
    ~Value() {}

  public:
    std::unique_ptr<Any> value;
    TraceContext* traceContext;
    ValueRef ref;
    ValueRef srcRef;
};

template<class T>
concept IsValueType = std::is_same<Value, T>::value;

template<class T>
concept IsNotValueType = !
std::is_same<Value, T>::value;

template<class T>
    requires(std::is_fundamental<T>::value == true)
inline auto toValue(T&& t, TraceContext* traceContext) -> Value {
    return Value{std::forward<T>(t), traceContext};
}

template<IsNotValueType LHS, IsValueType RHS>
auto inline operator+(const LHS& left, const RHS& right) {
    auto leftValue = toValue(std::forward<const LHS>(left), right.traceContext);
    return leftValue + right;
};

template<IsValueType LHS, IsNotValueType RHS>
auto inline operator+(const LHS& left, const RHS& right) {
    auto rightValue = toValue(std::forward<const RHS>(right), left.traceContext);
    return left + rightValue;
};

template<IsValueType LHS, IsValueType RHS>
auto inline operator+(const LHS& left, const RHS& right) {
    auto result = Operations::AddOp(left.value, right.value);
    auto resValue = Value(std::move(result), left.traceContext);
    Trace(left.traceContext, OpCode::ADD, left, right, resValue);
    return resValue;
};

template<IsNotValueType LHS, IsValueType RHS>
auto inline operator-(const LHS& left, const RHS& right) {
    auto leftValue = toValue(std::forward<const LHS>(left), right.traceContext);
    return leftValue - right;
};

template<IsValueType LHS, IsNotValueType RHS>
auto inline operator-(const LHS& left, const RHS& right) {
    auto rightValue = toValue(std::forward<const RHS>(right), left.traceContext);
    return left - rightValue;
};

template<IsValueType LHS, IsValueType RHS>
auto inline operator-(const LHS& left, const RHS& right) {
    auto result = Operations::SubOp(left.value, right.value);
    auto resValue = Value(std::move(result), left.traceContext);
    Trace(left.traceContext, OpCode::SUB, left, right, resValue);
    return resValue;
};

template<IsNotValueType LHS, IsValueType RHS>
auto inline operator*(const LHS& left, const RHS& right) {
    auto leftValue = toValue(std::forward<const LHS>(left), right.traceContext);
    return leftValue * right;
};

template<IsValueType LHS, IsNotValueType RHS>
auto inline operator*(const LHS& left, const RHS& right) {
    auto rightValue = toValue(std::forward<const RHS>(right), left.traceContext);
    return left * rightValue;
};

template<IsValueType LHS, IsValueType RHS>
auto inline operator*(const LHS& left, const RHS& right) {
    auto result = Operations::MulOp(left.value, right.value);
    auto resValue = Value(std::move(result), left.traceContext);
    Trace(left.traceContext, OpCode::MUL, left, right, resValue);
    return resValue;
};

template<IsNotValueType LHS, IsValueType RHS>
auto inline operator/(const LHS& left, const RHS& right) {
    auto leftValue = toValue(std::forward<const LHS>(left), right.traceContext);
    return leftValue / right;
};

template<IsValueType LHS, IsNotValueType RHS>
auto inline operator/(const LHS& left, const RHS& right) {
    auto rightValue = toValue(std::forward<const RHS>(right), left.traceContext);
    return left / rightValue;
};

template<IsValueType LHS, IsValueType RHS>
auto inline operator/(const LHS& left, const RHS& right) {
    auto result = Operations::DivOp(left.value, right.value);
    auto resValue = Value(std::move(result), left.traceContext);
    Trace(left.traceContext, OpCode::DIV, left, right, resValue);
    return resValue;
};

// --- logical operations ----

template<IsNotValueType LHS, IsValueType RHS>
auto inline operator==(const LHS& left, const RHS& right) {
    auto leftValue = toValue(std::forward<const LHS>(left), right.traceContext);
    return leftValue == right;
};

template<IsValueType LHS, IsNotValueType RHS>
auto inline operator==(const LHS& left, const RHS& right) {
    auto rightValue = toValue(std::forward<const RHS>(right), left.traceContext);
    return left == rightValue;
};

template<IsValueType LHS, IsValueType RHS>
auto inline operator==(const LHS& left, const RHS& right) {
    auto result = Operations::EqualsOp(left.value, right.value);
    auto resValue = Value(std::move(result), left.traceContext);
    Trace(left.traceContext, OpCode::EQUALS, left, right, resValue);
    return resValue;
};

template<IsValueType LHS>
auto inline operator!(const LHS& left) {
    auto result = Operations::NegateOp(left.value);
    auto resValue = Value(std::move(result), left.traceContext);
    Trace(left.traceContext, OpCode::NEGATE, left, resValue);
    return resValue;
};

template<IsNotValueType LHS, IsValueType RHS>
auto inline operator!=(const LHS& left, const RHS& right) {
    auto leftValue = toValue(std::forward<const LHS>(left), right.traceContext);
    return leftValue != right;
};

template<IsValueType LHS, IsNotValueType RHS>
auto inline operator!=(const LHS& left, const RHS& right) {
    auto rightValue = toValue(std::forward<const RHS>(right), left.traceContext);
    return left != rightValue;
};

template<IsValueType LHS, IsValueType RHS>
auto inline operator!=(const LHS& left, const RHS& right) {
    return !(left == right);
};

template<IsNotValueType LHS, IsValueType RHS>
auto inline operator<(const LHS& left, const RHS& right) {
    auto leftValue = toValue(std::forward<const LHS>(left), right.traceContext);
    return leftValue < right;
};

template<IsValueType LHS, IsNotValueType RHS>
auto inline operator<(const LHS& left, const RHS& right) {
    auto rightValue = toValue(std::forward<const RHS>(right), left.traceContext);
    return left < rightValue;
};

template<IsValueType LHS, IsValueType RHS>
auto inline operator<(const LHS& left, const RHS& right) {
    auto result = Operations::LessThan(left.value, right.value);
    auto resValue = Value(std::move(result), left.traceContext);
    Trace(left.traceContext, OpCode::LESS_THAN, left, right, resValue);
    return resValue;
};

template<IsNotValueType LHS, IsValueType RHS>
auto inline operator>(const LHS& left, const RHS& right) {
    auto leftValue = toValue(std::forward<const LHS>(left), right.traceContext);
    return leftValue > right;
};

template<IsValueType LHS, IsNotValueType RHS>
auto inline operator>(const LHS& left, const RHS& right) {
    auto rightValue = toValue(std::forward<const RHS>(right), left.traceContext);
    return left > rightValue;
};

template<IsValueType LHS, IsValueType RHS>
auto inline operator>(const LHS& left, const RHS& right) {
    return !(left <= right);
};

template<IsNotValueType LHS, IsValueType RHS>
auto inline operator>=(const LHS& left, const RHS& right) {
    auto leftValue = toValue(std::forward<const LHS>(left), right.traceContext);
    return leftValue >= right;
};

template<IsValueType LHS, IsNotValueType RHS>
auto inline operator>=(const LHS& left, const RHS& right) {
    auto rightValue = toValue(std::forward<const RHS>(right), left.traceContext);
    return left >= rightValue;
};

template<IsValueType LHS, IsValueType RHS>
auto inline operator>=(const LHS& left, const RHS& right) {
    return !(left < right);
};

template<IsNotValueType LHS, IsValueType RHS>
auto inline operator<=(const LHS& left, const RHS& right) {
    auto leftValue = toValue(std::forward<const LHS>(left), right.traceContext);
    return leftValue <= right;
};

template<IsValueType LHS, IsNotValueType RHS>
auto inline operator<=(const LHS& left, const RHS& right) {
    auto rightValue = toValue(std::forward<const RHS>(right), left.traceContext);
    return left <= rightValue;
};

template<IsValueType LHS, IsValueType RHS>
auto inline operator<=(const LHS& left, const RHS& right) {
    return (left < right) || (left == right);
};

template<IsNotValueType LHS, IsValueType RHS>
auto inline operator&&(const LHS& left, const RHS& right) {
    auto leftValue = toValue(std::forward<const LHS>(left), right.traceContext);
    return leftValue && right;
};

template<IsValueType LHS, IsNotValueType RHS>
auto inline operator&&(const LHS& left, const RHS& right) {
    auto rightValue = toValue(std::forward<const RHS>(right), left.traceContext);
    return left && rightValue;
};

template<IsValueType LHS, IsValueType RHS>
auto inline operator&&(const LHS& left, const RHS& right) {
    auto result = Operations::AndOp(left.value, right.value);
    auto resValue = Value(std::move(result), left.traceContext);
    Trace(left.traceContext, OpCode::AND, left, right, resValue);
    return resValue;
};

template<IsNotValueType LHS, IsValueType RHS>
auto inline operator||(const LHS& left, const RHS& right) {
    auto leftValue = toValue(std::forward<const LHS>(left), right.traceContext);
    return leftValue || right;
};

template<IsValueType LHS, IsNotValueType RHS>
auto inline operator||(const LHS& left, const RHS& right) {
    auto rightValue = toValue(std::forward<const RHS>(right), left.traceContext);
    return left || rightValue;
};

template<IsValueType LHS, IsValueType RHS>
auto inline operator||(const LHS& left, const RHS& right) {
    auto result = Operations::OrOp(left.value, right.value);
    auto resValue = Value(std::move(result), left.traceContext);
    Trace(left.traceContext, OpCode::OR, left, right, resValue);
    return resValue;
};

}// namespace NES::Interpreter

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_DATAVALUE_VALUE_HPP_

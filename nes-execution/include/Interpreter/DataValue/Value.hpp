#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_DATAVALUE_VALUE_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_DATAVALUE_VALUE_HPP_
#include <Interpreter/DataValue/Any.hpp>
#include <Interpreter/Operations/AddOp.hpp>
#include <Interpreter/Operations/AndOp.hpp>
#include <Interpreter/Operations/DivOp.hpp>
#include <Interpreter/Operations/EqualsOp.hpp>
#include <Interpreter/Operations/LessThenOp.hpp>
#include <Interpreter/Operations/MulOp.hpp>
#include <Interpreter/Operations/NegateOp.hpp>
#include <Interpreter/Operations/OrOp.hpp>
#include <Interpreter/Operations/SubOp.hpp>
#include <Interpreter/Util/Casting.hpp>
#include <Util/Logger/Logger.hpp>
#include <execinfo.h>
#include <memory>
namespace NES::Interpreter {

void Trace() {
    std::cout << "Trace" << std::endl;
    void* buffer[20];
    // First add the RIP pointers
    int backtrace_size = backtrace(buffer, 20);
    std::cout << buffer[0] << ";" << buffer[1] << ";" << buffer[2] << ";" << buffer[3] << ";" << buffer[4] << ";" << buffer[5]
              << ";" << buffer[6] << ";" << buffer[7] << ";" << buffer[8] <<";" << buffer[9] <<";" << buffer[10] << std::endl;
}


class Value {
  public:
    Value(std::unique_ptr<Any>& wrappedValue) : value(std::move(wrappedValue)){};
    Value(int64_t value) : value(std::make_unique<Integer>(value)){};

    // copy constructor
    Value(const Value& other) : value(other.value->copy()) {}

    // move constructor
    Value(Value&& other) noexcept : value(std::exchange(other.value, nullptr)) {}

    // copy assignment
    Value& operator=(const Value& other) { return *this = Value(other); }

    // move assignment
    Value& operator=(Value&& other) noexcept {
        std::swap(value, other.value);
        return *this;
    }

    operator bool() const {
        std::cout << "trace bool eval" << std::endl;
        if (instanceOf<Boolean>(value)) {
            return cast<Boolean>(value)->value;
        }
        return false;
    };

    // destructor
    ~Value() {}

  public:
    std::unique_ptr<Any> value;
};

template<class T>
requires(std::is_fundamental<T>::value == true) inline auto toValue(T t) -> Value { return Value(t); }

template<class T>
requires(std::is_same<std::decay_t<T>, Value>::value == true) inline auto toValue(T& t) -> const Value& { return t; }

template<typename LHS, typename RHS>
requires(std::is_same<LHS, Value>::value == false) auto inline operator+(const LHS& left, const RHS& right) {
    auto leftValue = toValue(left);
    return leftValue + right;
};

template<typename LHS, typename RHS>
requires(std::is_same<RHS, Value>::value == false) auto inline operator+(const LHS& left, const RHS& right) {
    auto rightValue = toValue(right);
    return left + rightValue;
};

template<typename LHS, typename RHS>
requires(std::is_same<RHS, Value>::value == true && std::is_same<RHS, Value>::value == true) auto inline
operator+(const LHS& left, const RHS& right) {
    std::cout << "Trace plus " << std::endl;
    Trace();
    auto result = Operations::AddOp(left.value, right.value);
    return Value(result);
};

template<typename LHS, typename RHS>
requires(std::is_same<LHS, Value>::value == false) auto inline operator-(const LHS& left, const RHS& right) {
    auto leftValue = toValue(left);
    return leftValue - right;
};

template<typename LHS, typename RHS>
requires(std::is_same<RHS, Value>::value == false) auto inline operator-(const LHS& left, const RHS& right) {
    auto rightValue = toValue(right);
    return left - rightValue;
};

template<typename LHS, typename RHS>
requires(std::is_same<RHS, Value>::value == true && std::is_same<RHS, Value>::value == true) auto inline
operator-(const LHS& left, const RHS& right) {
    std::cout << "Trace sub " << std::endl;
    Trace();
    auto result = Operations::SubOp(left.value, right.value);
    return Value(result);
};

template<typename LHS, typename RHS>
requires(std::is_same<LHS, Value>::value == false) auto inline operator*(const LHS& left, const RHS& right) {
    auto leftValue = toValue(left);
    return leftValue * right;
};

template<typename LHS, typename RHS>
requires(std::is_same<RHS, Value>::value == false) auto inline operator*(const LHS& left, const RHS& right) {
    auto rightValue = toValue(right);
    return left * rightValue;
};

template<typename LHS, typename RHS>
requires(std::is_same<RHS, Value>::value == true && std::is_same<RHS, Value>::value == true) auto inline
operator*(const LHS& left, const RHS& right) {
    std::cout << "Trace mul " << std::endl;
    Trace();
    auto result = Operations::MulOp(left.value, right.value);
    return Value(result);
};

template<typename LHS, typename RHS>
requires(std::is_same<LHS, Value>::value == false) auto inline operator/(const LHS& left, const RHS& right) {
    auto leftValue = toValue(left);
    return leftValue / right;
};

template<typename LHS, typename RHS>
requires(std::is_same<RHS, Value>::value == false) auto inline operator/(const LHS& left, const RHS& right) {
    auto rightValue = toValue(right);
    return left / rightValue;
};

template<typename LHS, typename RHS>
requires(std::is_same<RHS, Value>::value == true && std::is_same<RHS, Value>::value == true) auto inline
operator/(const LHS& left, const RHS& right) {
    std::cout << "Trace div " << std::endl;
    Trace();
    auto result = Operations::DivOp(left.value, right.value);
    return Value(result);
};

template<typename LHS, typename RHS>
requires(std::is_same<LHS, Value>::value == false) auto inline operator==(const LHS& left, const RHS& right) {
    auto leftValue = toValue(left);
    return leftValue == right;
};

template<typename LHS, typename RHS>
requires(std::is_same<RHS, Value>::value == false) auto inline operator==(const LHS& left, const RHS& right) {
    auto rightValue = toValue(right);
    return left == rightValue;
};

template<typename LHS, typename RHS>
requires(std::is_same<RHS, Value>::value == true && std::is_same<RHS, Value>::value == true) auto inline
operator==(const LHS& left, const RHS& right) {
    std::cout << "Trace equals " << std::endl;
    auto result = Operations::EqualsOp(left.value, right.value);
    return Value(result);
};

template<typename LHS>
requires(std::is_same<LHS, Value>::value == false) auto inline operator!(const LHS& left) { return !toValue(left); };

template<typename LHS>
requires(std::is_same<LHS, Value>::value == true) auto inline operator!(const LHS& left) {
    std::cout << "Trace negate " << std::endl;
    Trace();
    auto result = Operations::NegateOp(left.value);
    return Value(result);
};

template<typename LHS, typename RHS>
requires(std::is_same<LHS, Value>::value == false) auto inline operator!=(const LHS& left, const RHS& right) {
    auto leftValue = toValue(left);
    return leftValue != right;
};

template<typename LHS, typename RHS>
requires(std::is_same<RHS, Value>::value == false) auto inline operator!=(const LHS& left, const RHS& right) {
    auto rightValue = toValue(right);
    return left != rightValue;
};

template<typename LHS, typename RHS>
requires(std::is_same<LHS, Value>::value == true && std::is_same<RHS, Value>::value == true) auto inline
operator!=(const LHS& left, const RHS& right) {
    return !(left == right);
};

template<typename LHS, typename RHS>
requires(std::is_same<LHS, Value>::value == false) auto inline operator<(const LHS& left, const RHS& right) {
    auto leftValue = toValue(left);
    return leftValue < right;
};

template<typename LHS, typename RHS>
requires(std::is_same<RHS, Value>::value == false) auto inline operator<(const LHS& left, const RHS& right) {
    auto rightValue = toValue(right);
    return left < rightValue;
};

template<typename LHS, typename RHS>
requires(std::is_same<RHS, Value>::value == true && std::is_same<RHS, Value>::value == true) auto inline
operator<(const LHS& left, const RHS& right) {
    std::cout << "Trace less then " << std::endl;
    Trace();
    auto result = Operations::LessThan(left.value, right.value);
    return Value(result);
};

template<typename LHS, typename RHS>
requires(std::is_same<LHS, Value>::value == false) auto inline operator&&(const LHS& left, const RHS& right) {
    auto leftValue = toValue(left);
    return leftValue && right;
};

template<typename LHS, typename RHS>
requires(std::is_same<RHS, Value>::value == false) auto inline operator&&(const LHS& left, const RHS& right) {
    auto rightValue = toValue(right);
    return left && rightValue;
};

template<typename LHS, typename RHS>
requires(std::is_same<RHS, Value>::value == true && std::is_same<RHS, Value>::value == true) auto inline
operator&&(const LHS& left, const RHS& right) {
    std::cout << "Trace and " << std::endl;
    Trace();
    auto result = Operations::AndOp(left.value, right.value);
    return Value(result);
};

template<typename LHS, typename RHS>
requires(std::is_same<LHS, Value>::value == false) auto inline operator||(const LHS& left, const RHS& right) {
    auto leftValue = toValue(left);
    return leftValue || right;
};

template<typename LHS, typename RHS>
requires(std::is_same<RHS, Value>::value == false) auto inline operator||(const LHS& left, const RHS& right) {
    auto rightValue = toValue(right);
    return left || rightValue;
};

template<typename LHS, typename RHS>
requires(std::is_same<RHS, Value>::value == true && std::is_same<RHS, Value>::value == true) auto inline
operator||(const LHS& left, const RHS& right) {
    std::cout << "Trace or " << std::endl;
    Trace();
    auto result = Operations::OrOp(left.value, right.value);
    return Value(result);
};

template<typename LHS, typename RHS>
requires(std::is_same<LHS, Value>::value == false) auto inline operator<=(const LHS& left, const RHS& right) {
    auto leftValue = toValue(left);
    return leftValue <= right;
};

template<typename LHS, typename RHS>
requires(std::is_same<RHS, Value>::value == false) auto inline operator<=(const LHS& left, const RHS& right) {
    auto rightValue = toValue(right);
    return left <= rightValue;
};

template<typename LHS, typename RHS>
requires(std::is_same<RHS, Value>::value == true && std::is_same<RHS, Value>::value == true) auto inline
operator<=(const LHS& left, const RHS& right) {
    return (left < right) || (left == right);
};

}// namespace NES::Interpreter

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_DATAVALUE_VALUE_HPP_

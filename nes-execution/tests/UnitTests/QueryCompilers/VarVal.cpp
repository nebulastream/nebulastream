//
// Created by ls on 12/1/24.
//

#include "VarVal.hpp"
namespace NES::Nautilus
{
#define DEFINE_OPERATOR_VAR_VAL_BINARY(operatorName, op) \
    ScalarVarVal ScalarVarVal::operatorName(const ScalarVarVal& rhs) const \
    { \
        return std::visit( \
            [&]<typename LHS, typename RHS>(const LHS& lhsVal, const RHS& rhsVal) \
            { \
                if constexpr (requires(LHS l, RHS r) { l op r; }) \
                { \
                    return detail::var_val_t(lhsVal op rhsVal); \
                } \
                else \
                { \
                    throw UnsupportedOperation( \
                        std::string("ScalarVarVal operation not implemented: ") + " " + #operatorName + " " + typeid(LHS).name() + " " \
                        + typeid(RHS).name()); \
                    return detail::var_val_t(lhsVal); \
                } \
            }, \
            this->value, \
            rhs.value); \
    }

#define DEFINE_OPERATOR_VAR_VAL_UNARY(operatorName, op) \
    ScalarVarVal ScalarVarVal::operatorName() const \
    { \
        return std::visit( \
            [&]<typename RHS>(const RHS& rhsVal) \
            { \
                if constexpr (!requires(RHS r) { op r; }) \
                { \
                    throw UnsupportedOperation( \
                        std::string("ScalarVarVal operation not implemented: ") + " " + #operatorName + " " \
                        + typeid(decltype(rhsVal)).name()); \
                    return detail::var_val_t(rhsVal); \
                } \
                else \
                { \
                    detail::var_val_t result = op rhsVal; \
                    return result; \
                } \
            }, \
            this->value); \
    }

DEFINE_OPERATOR_VAR_VAL_BINARY(operator+, +);
DEFINE_OPERATOR_VAR_VAL_BINARY(operator-, -);
DEFINE_OPERATOR_VAR_VAL_BINARY(operator*, *);
DEFINE_OPERATOR_VAR_VAL_BINARY(operator/, /);
DEFINE_OPERATOR_VAR_VAL_BINARY(operator==, ==);
DEFINE_OPERATOR_VAR_VAL_BINARY(operator!=, !=);
DEFINE_OPERATOR_VAR_VAL_BINARY(operator&&, &&);
DEFINE_OPERATOR_VAR_VAL_BINARY(operator||, ||);
DEFINE_OPERATOR_VAR_VAL_BINARY(operator<, <);
DEFINE_OPERATOR_VAR_VAL_BINARY(operator>, >);
DEFINE_OPERATOR_VAR_VAL_BINARY(operator<=, <=);
DEFINE_OPERATOR_VAR_VAL_BINARY(operator>=, >=);
DEFINE_OPERATOR_VAR_VAL_BINARY(operator&, &);
DEFINE_OPERATOR_VAR_VAL_BINARY(operator|, |);
DEFINE_OPERATOR_VAR_VAL_BINARY(operator^, ^);
DEFINE_OPERATOR_VAR_VAL_BINARY(operator<<, <<);
DEFINE_OPERATOR_VAR_VAL_BINARY(operator>>, >>);
DEFINE_OPERATOR_VAR_VAL_UNARY(operator!, !);
ScalarVarVal::ScalarVarVal(const ScalarVarVal& other) : value(other.value)
{
}

ScalarVarVal::ScalarVarVal(ScalarVarVal&& other) noexcept : value(std::move(other.value))
{
}

ScalarVarVal::ScalarVarVal(const detail::var_val_t t) : value(std::move(t))
{
}
nautilus::val<bool> ScalarVarValPtr::operator==(const ScalarVarValPtr& other) const
{
    return this->cast<void>() == other.cast<void>();
}
ScalarVarValPtr ScalarVarValPtr::advanceBytes(nautilus::val<ssize_t> bytes) const
{
    return std::visit(
        [&]<typename T0>(T0& value) { return ScalarVarValPtr(static_cast<T0>(static_cast<nautilus::val<int8_t*>>(value) + bytes)); },
        this->value);
}
ScalarVarVal ScalarVarValPtr::operator*()
{
    return std::visit([]<typename T0>(T0& value) { return ScalarVarVal(nautilus::val<typename T0::ValType>(*value)); }, this->value);
}
VariableSizeVal::VariableSizeVal(ScalarVarValPtr data, nautilus::val<size_t> size, nautilus::val<size_t> numberOfElements)
    : data(std::move(data)), size(std::move(size)), numberOfElements(std::move(numberOfElements))
{
}
nautilus::val<bool> VarVal::operator==(const VarVal& other) const
{
    return std::visit(
               []<typename T0, typename T1>(T0& lhs, T1& rhs) -> ScalarVarVal
               {
                   if constexpr (!std::is_same_v<T0, T1>)
                   {
                       throw std::runtime_error("Type mismatch");
                   }
                   else
                   {
                       return lhs == rhs;
                   }
               },
               this->value,
               other.value)
        .cast<nautilus::val<bool>>();
}
ScalarVarVal::operator bool() const
{
    return std::visit(
        []<typename T>(T& val) -> nautilus::val<bool>
        {
            if constexpr (requires(T t) { t == nautilus::val<bool>(true); })
            {
                /// We have to do it like this. The reason is that during the comparison of the two values, @val is NOT converted to a bool
                /// but rather the val<bool>(false) is converted to std::common_type<T, bool>. This is a problem for any val that is not set to 1.
                /// As we will then compare val == 1, which will always be false.
                return !(val == nautilus::val<bool>(false));
            }
            else
            {
                throw UnsupportedOperation();
                return nautilus::val<bool>(false);
            }
        },
        value);
}
nautilus::val<bool> VariableSizeVal::operator==(const VariableSizeVal& other) const
{
    return this->numberOfElements == other.numberOfElements && this->size == other.size
        && nautilus::memcmp(data.cast<void>(), other.data.cast<void>(), this->size) == 0;
}
}
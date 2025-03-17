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

#include <cstdint>
#include <memory>
#include <type_traits>

#include <DataTypes/DataTypeUtil.hpp>
#include <Nautilus/DataTypes/DataTypesUtil.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <fmt/format.h>
#include <magic_enum/magic_enum.hpp>
#include <nautilus/std/ostream.h>
#include <nautilus/val.hpp>
#include <nautilus/val_ptr.hpp>
#include <ErrorHandling.hpp>


namespace NES::Nautilus
{

VarVal::VarVal(const VarVal& other) : value(other.value)
{
}

VarVal::VarVal(VarVal&& other) noexcept : value(std::move(other.value))
{
}

VarVal& VarVal::operator=(const VarVal& other)
{
    value = other.value;
    return *this;
}

VarVal& VarVal::operator=(VarVal&& other) noexcept
{
    value = std::move(other.value);
    return *this;
}

void VarVal::writeToMemory(const nautilus::val<int8_t*>& memRef) const
{
    std::visit(
        [&]<typename ValType>(const ValType& val)
        {
            if constexpr (std::is_same_v<ValType, VariableSizedData>)
            {
                throw UnsupportedOperation(std::string("VarVal T::operation=(val) not implemented for VariableSizedData"));
                return;
            }
            else
            {
                *static_cast<nautilus::val<typename ValType::raw_type*>>(memRef) = val;
                return;
            }
        },
        value);
}

VarVal::operator bool() const
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

VarVal VarVal::castToType(const DataType::Type type) const
{
    if (type == DataType::Type::VARSIZED)
    {
        return cast<VariableSizedData>();
    }
    return DataTypeUtil::dispatchByNumericalTypeOrBool(type, [this]<typename T>() { return VarVal{cast<nautilus::val<T>>()}; });
}

VarVal VarVal::readVarValFromMemory(const nautilus::val<int8_t*>& memRef, DataType::Type type)
{
    return DataTypeUtil::dispatchByNumericalTypeOrBool(type, [&memRef]<typename T>() { return VarVal{Util::readValueFromMemRef<T>(memRef)}; });
}

nautilus::val<std::ostream>& operator<<(nautilus::val<std::ostream>& os, const VarVal& varVal)
{
    return std::visit(
        [&os]<typename T>(T& value) -> nautilus::val<std::ostream>&
        {
            /// If the T is of type uint8_t or int8_t, we want to convert it to an integer to print it as an integer and not as a char
            using Tremoved = std::remove_cvref_t<T>;
            if constexpr (
                std::is_same_v<Tremoved, nautilus::val<uint8_t>> || std::is_same_v<Tremoved, nautilus::val<int8_t>>
                || std::is_same_v<Tremoved, nautilus::val<unsigned char>> || std::is_same_v<Tremoved, nautilus::val<char>>)
            {
                return os.operator<<(static_cast<nautilus::val<int>>(value));
            }
            else if constexpr (requires(typename T::basic_type type) { os << (nautilus::val<typename T::basic_type>(type)); })
            {
                return os.operator<<(nautilus::val<typename T::basic_type>(value));
            }
            else if constexpr (requires { operator<<(os, value); })
            {
                return operator<<(os, value);
            }
            else
            {
                throw UnsupportedOperation();
                return os;
            }
        },
        varVal.value);
}

}

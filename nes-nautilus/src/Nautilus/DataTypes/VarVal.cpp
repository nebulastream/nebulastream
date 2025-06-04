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
#include <Nautilus/DataTypes/VarVal.hpp>

#include <cstdint>
#include <ostream>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>
#include <DataTypes/DataType.hpp>
#include <Nautilus/DataTypes/DataTypesUtil.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <magic_enum/magic_enum.hpp>
#include <nautilus/std/ostream.h>
#include <nautilus/val.hpp>
#include <nautilus/val_ptr.hpp>
#include <ErrorHandling.hpp>
#include <val_concepts.hpp>


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
    if (value.index() != other.value.index())
    {
        throw UnknownOperation("Not allowed to change the data type via the assignment operator, please use castToType()!");
    }
    value = other.value;
    return *this;
}

VarVal& VarVal::operator=(VarVal&& other) /// NOLINT, as we need to have the option of throwing
{
    if (value.index() != other.value.index())
    {
        throw UnknownOperation("Not allowed to change the data type via the assignment operator, please use castToType()!");
    }
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
                throw UnknownOperation(std::string("VarVal T::operation=(val) not implemented for VariableSizedData"));
            }
            else
            {
                *static_cast<nautilus::val<typename ValType::raw_type*>>(memRef) = val;
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
                throw UnknownOperation();
            }
        },
        value);
}

VarVal VarVal::castToType(const DataType::Type type) const
{
    switch (type)
    {
        case DataType::Type::BOOLEAN: {
            return {cast<nautilus::val<bool>>()};
        }
        case DataType::Type::INT8: {
            return {cast<nautilus::val<int8_t>>()};
        }
        case DataType::Type::INT16: {
            return {cast<nautilus::val<int16_t>>()};
        }
        case DataType::Type::INT32: {
            return {cast<nautilus::val<int32_t>>()};
        }
        case DataType::Type::INT64: {
            return {cast<nautilus::val<int64_t>>()};
        }
        case DataType::Type::UINT8: {
            return {cast<nautilus::val<uint8_t>>()};
        }
        case DataType::Type::UINT16: {
            return {cast<nautilus::val<uint16_t>>()};
        }
        case DataType::Type::UINT32: {
            return {cast<nautilus::val<uint32_t>>()};
        }
        case DataType::Type::UINT64: {
            return {cast<nautilus::val<uint64_t>>()};
        }
        case DataType::Type::FLOAT32: {
            return {cast<nautilus::val<float>>()};
        }
        case DataType::Type::FLOAT64: {
            return {cast<nautilus::val<double>>()};
        }
        case DataType::Type::VARSIZED: {
            return cast<VariableSizedData>();
        }
        case DataType::Type::VARSIZED_POINTER_REP:
            throw UnknownDataType(
                "Not supporting reading {} data type from memory. VARSIZED_POINTER_REP should is only supported in the ChainedHashMap!",
                magic_enum::enum_name(type));
        case DataType::Type::CHAR:
        case DataType::Type::UNDEFINED:
            throw UnknownDataType("Not supporting reading {} data type from memory.", magic_enum::enum_name(type));
    }
    std::unreachable();
}

VarVal VarVal::readVarValFromMemory(const nautilus::val<int8_t*>& memRef, const DataType::Type type)
{
    switch (type)
    {
        case DataType::Type::BOOLEAN: {
            return {Util::readValueFromMemRef<bool>(memRef)};
        }
        case DataType::Type::INT8: {
            return {Util::readValueFromMemRef<int8_t>(memRef)};
        }
        case DataType::Type::INT16: {
            return {Util::readValueFromMemRef<int16_t>(memRef)};
        }
        case DataType::Type::INT32: {
            return {Util::readValueFromMemRef<int32_t>(memRef)};
        }
        case DataType::Type::INT64: {
            return {Util::readValueFromMemRef<int64_t>(memRef)};
        }
        case DataType::Type::UINT8: {
            return {Util::readValueFromMemRef<uint8_t>(memRef)};
        }
        case DataType::Type::UINT16: {
            return {Util::readValueFromMemRef<uint16_t>(memRef)};
        }
        case DataType::Type::UINT32: {
            return {Util::readValueFromMemRef<uint32_t>(memRef)};
        }
        case DataType::Type::UINT64: {
            return {Util::readValueFromMemRef<uint64_t>(memRef)};
        }
        case DataType::Type::FLOAT32: {
            return {Util::readValueFromMemRef<float>(memRef)};
        }
        case DataType::Type::FLOAT64: {
            return {Util::readValueFromMemRef<double>(memRef)};
        }
        case DataType::Type::CHAR:
        case DataType::Type::VARSIZED:
        case DataType::Type::VARSIZED_POINTER_REP:
        case DataType::Type::UNDEFINED:
            throw UnknownDataType("Not supporting reading {} data type from memory.", magic_enum::enum_name(type));
    }
    std::unreachable();
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
                throw UnknownOperation();
                return os;
            }
        },
        varVal.value);
}

}

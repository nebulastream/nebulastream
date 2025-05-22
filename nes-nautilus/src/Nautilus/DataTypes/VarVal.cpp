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
#include <Nautilus/DataTypes/DataTypesUtil.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <fmt/format.h>
#include <nautilus/std/ostream.h>
#include <nautilus/val.hpp>
#include <nautilus/val_ptr.hpp>
#include <ErrorHandling.hpp>
#include <Common/DataTypes/VariableSizedDataType.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>

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

VarVal VarVal::castToType(const std::shared_ptr<PhysicalType>& type) const
{
    if (const auto basicType = std::dynamic_pointer_cast<BasicPhysicalType>(type))
    {
        switch (basicType->nativeType)
        {
            case BasicPhysicalType::NativeType::BOOLEAN: {
                return {cast<nautilus::val<bool>>()};
            };
            case BasicPhysicalType::NativeType::INT_8: {
                return {cast<nautilus::val<int8_t>>()};
            };
            case BasicPhysicalType::NativeType::INT_16: {
                return {cast<nautilus::val<int16_t>>()};
            };
            case BasicPhysicalType::NativeType::INT_32: {
                return {cast<nautilus::val<int32_t>>()};
            };
            case BasicPhysicalType::NativeType::INT_64: {
                return {cast<nautilus::val<int64_t>>()};
            };
            case BasicPhysicalType::NativeType::UINT_8: {
                return {cast<nautilus::val<uint8_t>>()};
            };
            case BasicPhysicalType::NativeType::UINT_16: {
                return {cast<nautilus::val<uint16_t>>()};
            };
            case BasicPhysicalType::NativeType::UINT_32: {
                return {cast<nautilus::val<uint32_t>>()};
            };
            case BasicPhysicalType::NativeType::UINT_64: {
                return {cast<nautilus::val<uint64_t>>()};
            };
            case BasicPhysicalType::NativeType::FLOAT: {
                return {cast<nautilus::val<float>>()};
            };
            case BasicPhysicalType::NativeType::DOUBLE: {
                return {cast<nautilus::val<double>>()};
            };
            default: {
                throw UnsupportedOperation(fmt::format("Physical Type: {} is currently not supported", type->toString()));
            };
        }
    }
    else if (const auto variableSizedData = std::dynamic_pointer_cast<VariableSizedDataType>(type))
    {
        return cast<VariableSizedData>();
    }
    else
    {
        throw UnsupportedOperation(fmt::format("Type: {} is not a basic type", type->toString()));
    }
}

VarVal VarVal::readVarValFromMemory(const nautilus::val<int8_t*>& memRef, const std::shared_ptr<PhysicalType>& type)
{
    if (const auto basicType = std::static_pointer_cast<BasicPhysicalType>(type))
    {
        switch (basicType->nativeType)
        {
            case BasicPhysicalType::NativeType::BOOLEAN: {
                return {Util::readValueFromMemRef<bool>(memRef)};
            };
            case BasicPhysicalType::NativeType::INT_8: {
                return {Util::readValueFromMemRef<int8_t>(memRef)};
            };
            case BasicPhysicalType::NativeType::INT_16: {
                return {Util::readValueFromMemRef<int16_t>(memRef)};
            };
            case BasicPhysicalType::NativeType::INT_32: {
                return {Util::readValueFromMemRef<int32_t>(memRef)};
            };
            case BasicPhysicalType::NativeType::INT_64: {
                return {Util::readValueFromMemRef<int64_t>(memRef)};
            };
            case BasicPhysicalType::NativeType::UINT_8: {
                return {Util::readValueFromMemRef<uint8_t>(memRef)};
            };
            case BasicPhysicalType::NativeType::UINT_16: {
                return {Util::readValueFromMemRef<uint16_t>(memRef)};
            };
            case BasicPhysicalType::NativeType::UINT_32: {
                return {Util::readValueFromMemRef<uint32_t>(memRef)};
            };
            case BasicPhysicalType::NativeType::UINT_64: {
                return {Util::readValueFromMemRef<uint64_t>(memRef)};
            };
            case BasicPhysicalType::NativeType::FLOAT: {
                return {Util::readValueFromMemRef<float>(memRef)};
            };
            case BasicPhysicalType::NativeType::DOUBLE: {
                return {Util::readValueFromMemRef<double>(memRef)};
            };
            default: {
                throw UnsupportedOperation(fmt::format("Physical Type: {} is currently not supported", type->toString()));
            };
        }
    }
    else
    {
        throw UnsupportedOperation(fmt::format("Type: {} is not a basic type", type->toString()));
    }
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

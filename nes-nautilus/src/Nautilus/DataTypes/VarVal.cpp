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

VarVal::VarVal(const VarVal& other) : value(other.value), null(other.null), nullable(other.nullable)
{
}

VarVal::VarVal(VarVal&& other) noexcept : value(std::move(other.value)), null(std::move(other.null)), nullable(std::move(other.nullable))
{
}

VarVal& VarVal::operator=(const VarVal& other)
{
    value = other.value;
    null = other.null;
    nullable = other.nullable;
    return *this;
}

VarVal& VarVal::operator=(VarVal&& other) noexcept
{
    value = std::move(other.value);
    null = std::move(other.null);
    nullable = std::move(other.nullable);
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
                if (nullable)
                {
                    const auto memRefNull = memRef + nautilus::val<uint64_t>(sizeof(typename ValType::raw_type));
                    *static_cast<nautilus::val<bool*>>(memRefNull) = null;
                }
                return;
            }
        },
        value);
}

nautilus::val<bool> VarVal::isNull() const
{
    if (not nullable)
    {
        return false;
    }
    return null;
}

VarVal::operator bool() const
{
    return std::visit(
        [this]<typename T>(T& val) -> nautilus::val<bool>
        {
            if constexpr (requires(T t) { t == nautilus::val<bool>(true); })
            {
                /// We have to do it like this. The reason is that during the comparison of the two values, @val is NOT converted to a bool
                /// but rather the val<bool>(false) is converted to std::common_type<T, bool>. This is a problem for any val that is not set to 1.
                /// As we will then compare val == 1, which will always be false.
                return !null and !(val == nautilus::val<bool>(false));
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
                return {getRawValueAs<nautilus::val<bool>>(), null, nullable};
            };
            case BasicPhysicalType::NativeType::INT_8: {
                return {getRawValueAs<nautilus::val<int8_t>>(), null, nullable};
            };
            case BasicPhysicalType::NativeType::INT_16: {
                return {getRawValueAs<nautilus::val<int16_t>>(), null, nullable};
            };
            case BasicPhysicalType::NativeType::INT_32: {
                return {getRawValueAs<nautilus::val<int32_t>>(), null, nullable};
            };
            case BasicPhysicalType::NativeType::INT_64: {
                return {getRawValueAs<nautilus::val<int64_t>>(), null, nullable};
            };
            case BasicPhysicalType::NativeType::UINT_8: {
                return {getRawValueAs<nautilus::val<uint8_t>>(), null, nullable};
            };
            case BasicPhysicalType::NativeType::UINT_16: {
                return {getRawValueAs<nautilus::val<uint16_t>>(), null, nullable};
            };
            case BasicPhysicalType::NativeType::UINT_32: {
                return {getRawValueAs<nautilus::val<uint32_t>>(), null, nullable};
            };
            case BasicPhysicalType::NativeType::UINT_64: {
                return {getRawValueAs<nautilus::val<uint64_t>>(), null, nullable};
            };
            case BasicPhysicalType::NativeType::FLOAT: {
                return {getRawValueAs<nautilus::val<float>>(), null, nullable};
            };
            case BasicPhysicalType::NativeType::DOUBLE: {
                return {getRawValueAs<nautilus::val<double>>(), null, nullable};
            };
            default: {
                throw UnsupportedOperation(fmt::format("Physical Type: {} is currently not supported", type->toString()));
            };
        }
    }
    else if (const auto variableSizedData = std::dynamic_pointer_cast<VariableSizedDataType>(type))
    {
        return {getRawValueAs<VariableSizedData>(), null, nullable};
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
        /// Depending on the type, we have to read a boolean after the memRef that stores the null value
        nautilus::val<bool> null = false;
        if (type->type->nullable)
        {
            const auto memRefNull = memRef + nautilus::val<uint64_t>(type->size());
            null = Util::readValueFromMemRef<bool>(memRefNull);
        }
        switch (basicType->nativeType)
        {
            case BasicPhysicalType::NativeType::BOOLEAN: {
                return {Util::readValueFromMemRef<bool>(memRef), null, type->type->nullable};
            };
            case BasicPhysicalType::NativeType::INT_8: {
                return {Util::readValueFromMemRef<int8_t>(memRef), null, type->type->nullable};
            };
            case BasicPhysicalType::NativeType::INT_16: {
                return {Util::readValueFromMemRef<int16_t>(memRef), null, type->type->nullable};
            };
            case BasicPhysicalType::NativeType::INT_32: {
                return {Util::readValueFromMemRef<int32_t>(memRef), null, type->type->nullable};
            };
            case BasicPhysicalType::NativeType::INT_64: {
                return {Util::readValueFromMemRef<int64_t>(memRef), null, type->type->nullable};
            };
            case BasicPhysicalType::NativeType::UINT_8: {
                const auto varVal = VarVal{Util::readValueFromMemRef<uint8_t>(memRef), null, type->type->nullable};
                return varVal;
            };
            case BasicPhysicalType::NativeType::UINT_16: {
                return {Util::readValueFromMemRef<uint16_t>(memRef), null, type->type->nullable};
            };
            case BasicPhysicalType::NativeType::UINT_32: {
                return {Util::readValueFromMemRef<uint32_t>(memRef), null, type->type->nullable};
            };
            case BasicPhysicalType::NativeType::UINT_64: {
                return {Util::readValueFromMemRef<uint64_t>(memRef), null, type->type->nullable};
            };
            case BasicPhysicalType::NativeType::FLOAT: {
                return {Util::readValueFromMemRef<float>(memRef), null, type->type->nullable};
            };
            case BasicPhysicalType::NativeType::DOUBLE: {
                return {Util::readValueFromMemRef<double>(memRef), null, type->type->nullable};
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
        [&os, varVal]<typename T>(T& value) -> nautilus::val<std::ostream>&
        {
            /// If the T is of type uint8_t or int8_t, we want to convert it to an integer to print it as an integer and not as a char
            using Tremoved = std::remove_cvref_t<T>;
            const auto nullString = varVal.null ? " (null)" : "";
            if constexpr (
                std::is_same_v<Tremoved, nautilus::val<uint8_t>> || std::is_same_v<Tremoved, nautilus::val<int8_t>>
                || std::is_same_v<Tremoved, nautilus::val<unsigned char>> || std::is_same_v<Tremoved, nautilus::val<char>>)
            {
                return os.operator<<(static_cast<nautilus::val<int>>(value)) << nullString;
            }
            else if constexpr (requires(typename T::basic_type type) { os << (nautilus::val<typename T::basic_type>(type)); })
            {
                return os.operator<<(nautilus::val<typename T::basic_type>(value)) << nullString;
            }
            else if constexpr (requires { operator<<(os, value); })
            {
                return operator<<(os, value) << nullString;
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

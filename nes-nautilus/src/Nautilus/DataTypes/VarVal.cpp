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

#include <memory>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <nautilus/std/cstring.h>
#include <nautilus/val.hpp>
#include <nautilus/val_ptr.hpp>
#include <ErrorHandling.hpp>
#include "Common/PhysicalTypes/PhysicalType.hpp"
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>

namespace NES::Nautilus
{

VarVal::VarVal(const VarVal& other) = default;
VarVal::VarVal(VarVal&& other) noexcept : value(std::move(other.value)), null(std::move(other.null))
{
}

VarVal& VarVal::operator=(const VarVal& other)
{
    if (this == &other)
    {
        return *this;
    }

    if (value.index() != other.value.index())
    {
        throw std::runtime_error("VarVal copy assignment with mismatching types.");
    }
    value = other.value;
    null = other.null;
    return *this;
}

VarVal& VarVal::operator=(VarVal&& other)
{
    if (this == &other)
    {
        return *this;
    }

    if (value.index() != other.value.index())
    {
        throw std::runtime_error("VarVal move assignment with mismatching types.");
    }
    value = std::move(other.value);
    null = std::move(other.null);
    return *this;
}

void VarVal::writeToMemory(nautilus::val<int8_t*>& memRef) const
{
    std::visit(
        [&]<typename ValType>(const ValType& val)
        {
            if constexpr (std::is_same_v<ValType, VariableSizedData>)
            {
                throw std::runtime_error(std::string("VarVal T::operation=(val) not implemented for VariableSizedData"));
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
    const auto isNotNull = !isNull();
    const auto valueIsTrue = std::visit(
        []<typename T>(T& val) -> nautilus::val<bool>
        {
            if constexpr (requires(T t) { t == nautilus::val<bool>(true); })
            {
                /// We have to do it like this. The reason is that during the comparison of the two values, @val is NOT converted to a bool
                /// but rather the val<bool(false) is converted to std::common_type<T, bool>. This is a problem for any val that is not set to 1.
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
    const auto result = isNotNull && valueIsTrue;
    return result;
}

[[nodiscard]] const nautilus::val<bool>& VarVal::isNull() const
{
    return null;
}

VarVal VarVal::readVarValFromMemory(const nautilus::val<int8_t*>& memRef, const PhysicalTypePtr& type)
{
    if (type->isBasicType())
    {
        switch (const auto basicType = std::static_pointer_cast<BasicPhysicalType>(type); basicType->nativeType)
        {
            case BasicPhysicalType::NativeType::BOOLEAN: {
                return {readValueFromMemRef(memRef, bool), nautilus::val<bool>(false)};
            };
            case BasicPhysicalType::NativeType::INT_8: {
                return {readValueFromMemRef(memRef, int8_t), nautilus::val<bool>(false)};
            };
            case BasicPhysicalType::NativeType::INT_16: {
                return {readValueFromMemRef(memRef, int16_t), nautilus::val<bool>(false)};
            };
            case BasicPhysicalType::NativeType::INT_32: {
                return {readValueFromMemRef(memRef, int32_t), nautilus::val<bool>(false)};
            };
            case BasicPhysicalType::NativeType::INT_64: {
                return {readValueFromMemRef(memRef, int64_t), nautilus::val<bool>(false)};
            };
            case BasicPhysicalType::NativeType::UINT_8: {
                return {readValueFromMemRef(memRef, uint8_t), nautilus::val<bool>(false)};
            };
            case BasicPhysicalType::NativeType::UINT_16: {
                return {readValueFromMemRef(memRef, uint16_t), nautilus::val<bool>(false)};
            };
            case BasicPhysicalType::NativeType::UINT_32: {
                return {readValueFromMemRef(memRef, uint32_t), nautilus::val<bool>(false)};
            };
            case BasicPhysicalType::NativeType::UINT_64: {
                return {readValueFromMemRef(memRef, uint64_t), nautilus::val<bool>(false)};
            };
            case BasicPhysicalType::NativeType::FLOAT: {
                return {readValueFromMemRef(memRef, float), nautilus::val<bool>(false)};
            };
            case BasicPhysicalType::NativeType::DOUBLE: {
                return {readValueFromMemRef(memRef, double), nautilus::val<bool>(false)};
            };
            default: {
                NES_ERROR("Physical Type: {} is currently not supported", type->toString());
                NES_NOT_IMPLEMENTED();
            };
        }
    }
    else
    {
        NES_ERROR("Physical Type: type {} is currently not supported", type->toString());
        NES_NOT_IMPLEMENTED();
    }
}

std::ostream& operator<<(std::ostream& os, const VarVal& varVal)
{
    if (varVal.isNull())
    {
        return os << "Null";
    }

    return std::visit(
        [&os]<typename T>(T& value) -> std::ostream&
        {
            if constexpr (requires(std::ostream& tOs, T t) { operator<<(tOs, t); })
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

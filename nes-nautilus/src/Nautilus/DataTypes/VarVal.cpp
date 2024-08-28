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

#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <nautilus/std/cstring.h>
#include <nautilus/val.hpp>
#include <nautilus/val_ptr.hpp>

namespace NES::Nautilus {

VarVal::VarVal(const VarVal& other) = default;
VarVal::VarVal(VarVal&& other) noexcept : value(std::move(other.value)), null(std::move(other.null)) {}

VarVal& VarVal::operator=(const VarVal& other) {
    if (value.index() != other.value.index()) {
        throw std::runtime_error("VarVal copy assignment with mismatching types.");
    }
    value = other.value;
    null = other.null;
    return *this;
}

VarVal& VarVal::operator=(VarVal&& other) {
    if (value.index() != other.value.index()) {
        throw std::runtime_error("VarVal move assignment with mismatching types.");
    }
    value = std::move(other.value);
    null = other.null;
    return *this;
}

void VarVal::writeVarValToMemRefVal(const MemRefVal& memRef) const {
    std::visit(
        [&]<typename ValType>(const ValType& val) {
            if constexpr (std::is_same_v<ValType, VariableSizedData>) {
                throw std::runtime_error(std::string("VarVal T::operation=(val) not implemented for VariableSizedData"));
                return;
            } else {
                *static_cast<nautilus::val<typename ValType::raw_type*>>(memRef) = val;
                return;
            }
        },
        value);
}

VarVal::operator bool() const {
    return !isNull()
        && std::visit(
            [](const auto& val) -> BooleanVal {
                return val == BooleanVal(true);
            },
            value);
}

[[nodiscard]] const nautilus::val<bool>& VarVal::isNull() const { return null; }

BooleanVal memEquals(const MemRefVal& ptr1, const MemRefVal& ptr2, const UInt64Val& size) {
    /// TODO #259 Somehow we have to write our own memEquals, as we otherwise get a error: 'llvm.call' op result type mismatch: '!llvm.ptr' != 'i32'

    // return nautilus::memcmp(ptr1, ptr2, size) == UInt64Val(0);

    for (UInt64Val i(0); i < size; ++i) {
        const auto tmp1 = static_cast<Int8Val>(*(ptr1 + i));
        const auto tmp2 = static_cast<Int8Val>(*(ptr2 + i));
        if (tmp1 != tmp2) {
            return {false};
        }
    }

    return {true};
}

void memCopy(const MemRefVal& dest, const MemRefVal& src, const nautilus::val<size_t>& size) {
    nautilus::memcpy(dest, src, size);
}

VarVal readVarValFromMemRef(const MemRefVal& memRef, const PhysicalTypePtr& type) {
    if (type->isBasicType()) {
        switch (const auto basicType = std::static_pointer_cast<BasicPhysicalType>(type); basicType->nativeType) {
            case BasicPhysicalType::NativeType::BOOLEAN: {
                // return {readValueFromMemRef(memRef, bool), BooleanVal(false)};
                return {static_cast<nautilus::val<bool>>(*static_cast<nautilus::val<bool*>>(memRef)), BooleanVal(false)};
            };
            case BasicPhysicalType::NativeType::INT_8: {
                return {readValueFromMemRef(memRef, int8_t), BooleanVal(false)};
            };
            case BasicPhysicalType::NativeType::INT_16: {
                return {readValueFromMemRef(memRef, int16_t), BooleanVal(false)};
            };
            case BasicPhysicalType::NativeType::INT_32: {
                return {readValueFromMemRef(memRef, int32_t), BooleanVal(false)};
            };
            case BasicPhysicalType::NativeType::INT_64: {
                return {readValueFromMemRef(memRef, int64_t), BooleanVal(false)};
            };
            case BasicPhysicalType::NativeType::UINT_8: {
                return {readValueFromMemRef(memRef, uint8_t), BooleanVal(false)};
            };
            case BasicPhysicalType::NativeType::UINT_16: {
                return {readValueFromMemRef(memRef, uint16_t), BooleanVal(false)};
            };
            case BasicPhysicalType::NativeType::UINT_32: {
                return {readValueFromMemRef(memRef, uint32_t), BooleanVal(false)};
            };
            case BasicPhysicalType::NativeType::UINT_64: {
                return {readValueFromMemRef(memRef, uint64_t), BooleanVal(false)};
            };
            case BasicPhysicalType::NativeType::FLOAT: {
                return {readValueFromMemRef(memRef, float), BooleanVal(false)};
            };
            case BasicPhysicalType::NativeType::DOUBLE: {
                return {readValueFromMemRef(memRef, double), BooleanVal(false)};
            };
            default: {
                NES_ERROR("Physical Type: {} is currently not supported", type->toString());
                NES_NOT_IMPLEMENTED();
            };
        }
    } else {
        NES_ERROR("Physical Type: type {} is currently not supported", type->toString());
        NES_NOT_IMPLEMENTED();
    }
}

std::ostream& operator<<(std::ostream& os, const VarVal& varVal) {
    /// We can later refine this by using a std::visit and calling toString() as toString() is implemented for all nautilus::val<T>
    /// We would have to implement a toString() for the VariableSizedDataType though
    return os << "NES::Nautilus::VarVal{null: " << varVal.null << "}";
}

}// namespace NES::Nautilus

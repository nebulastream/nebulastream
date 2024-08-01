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

#include <Nautilus/DataTypes/ExecutableDataType.hpp>
#include <Util/Logger/Logger.hpp>
#include <nautilus/val.hpp>
#include <nautilus/val_ptr.hpp>
#include <utility>

namespace NES::Nautilus {

// Define all operations on ExecutableDataType
template<typename ValueType>
ExecDataType ExecutableDataType<ValueType>::operator&&(const ExecDataType& rightExp) const {
    const auto resultIsNull = null || rightExp->isNull();
    const auto rawValueBool = static_cast<nautilus::val<bool>>(rawValue);
    const auto otherRawValueBool =
        static_cast<nautilus::val<bool>>(std::dynamic_pointer_cast<ExecutableDataType<ValueType>>(rightExp)->rawValue);
    return ExecutableDataType<bool>::create(rawValueBool && otherRawValueBool, resultIsNull);
}

template<typename ValueType>
ExecDataType ExecutableDataType<ValueType>::operator||(const ExecDataType& rightExp) const {
    if constexpr (std::is_same_v<ValueType, int8_t*>) {
        constexpr int8_t* nullPtr = nullptr;
        const auto rawValueBool = static_cast<nautilus::val<bool>>(rawValue == nullPtr);
        const auto otherRawValueBool = static_cast<nautilus::val<bool>>(
            std::dynamic_pointer_cast<ExecutableDataType<int8_t*>>(rightExp)->getRawValue() == nullPtr);
        const auto resultIsNull = null || rightExp->isNull();
        return ExecutableDataType<bool>::create(rawValueBool || otherRawValueBool, resultIsNull);
    } else {
        const auto rawValueBool = static_cast<nautilus::val<bool>>(rawValue);
        const auto otherRawValueBool =
            static_cast<nautilus::val<bool>>(std::dynamic_pointer_cast<ExecutableDataType<ValueType>>(rightExp)->rawValue);
        const auto resultIsNull = null || rightExp->isNull();
        return ExecutableDataType<bool>::create(rawValueBool || otherRawValueBool, resultIsNull);
    }
}

template<typename ValueType>
ExecDataType ExecutableDataType<ValueType>::operator==(const ExecDataType& rightExp) const {
    const auto otherRawValue = rightExp->as<ExecutableDataType<ValueType>>()->getRawValue();
    const auto resultIsNull = null || rightExp->isNull();
    const auto result = rawValue == otherRawValue;
    return ExecutableDataType<bool>::create(result, resultIsNull);
}

template<typename ValueType>
ExecDataType ExecutableDataType<ValueType>::operator!=(const ExecDataType& rightExp) const {
    const auto otherRawValue = rightExp->as<ExecutableDataType<ValueType>>()->getRawValue();
    const auto resultIsNull = null || rightExp->isNull();
    return ExecutableDataType<bool>::create(rawValue != otherRawValue, resultIsNull);
}

template<typename ValueType>
ExecDataType ExecutableDataType<ValueType>::operator<(const ExecDataType& rightExp) const {
    const auto otherRawValue = rightExp->as<ExecutableDataType<ValueType>>()->getRawValue();
    const auto resultIsNull = null || rightExp->isNull();
    return ExecutableDataType<bool>::create(rawValue < otherRawValue, resultIsNull);
}

template<typename ValueType>
ExecDataType ExecutableDataType<ValueType>::operator>(const ExecDataType& rightExp) const {
    const auto otherRawValue = rightExp->as<ExecutableDataType<ValueType>>()->getRawValue();
    const auto resultIsNull = null || rightExp->isNull();
    return ExecutableDataType<bool>::create(rawValue > otherRawValue, resultIsNull);
}

template<typename ValueType>
ExecDataType ExecutableDataType<ValueType>::operator<=(const ExecDataType& rightExp) const {
    const auto otherRawValue = rightExp->as<ExecutableDataType<ValueType>>()->getRawValue();
    const auto resultIsNull = null || rightExp->isNull();
    return ExecutableDataType<bool>::create(rawValue <= otherRawValue, resultIsNull);
}

template<typename ValueType>
ExecDataType ExecutableDataType<ValueType>::operator>=(const ExecDataType& rightExp) const {
    const auto otherRawValue = rightExp->as<ExecutableDataType<ValueType>>()->getRawValue();
    const auto resultIsNull = null || rightExp->isNull();
    return ExecutableDataType<bool>::create(rawValue >= otherRawValue, resultIsNull);
}

template<typename ValueType>
ExecDataType ExecutableDataType<ValueType>::operator+(const ExecDataType& rightExp) const {
    if constexpr (std::is_same_v<ValueType, bool>) {
        const auto rawValueInt = static_cast<nautilus::val<int64_t>>(rawValue);
        const auto otherRawValueInt = std::dynamic_pointer_cast<ExecutableDataType<int64_t>>(rightExp)->as<int64_t>();
        const auto resultIsNull = null || rightExp->isNull();
        return ExecutableDataType<int64_t>::create(rawValueInt + otherRawValueInt, resultIsNull);
    } else {
        const auto otherRawValue = rightExp->as<ExecutableDataType<ValueType>>()->getRawValue();
        const auto resultIsNull = null || rightExp->isNull();
        return ExecutableDataType<ValueType>::create(rawValue + otherRawValue, resultIsNull);
    }
}

template<typename ValueType>
ExecDataType ExecutableDataType<ValueType>::operator-(const ExecDataType& rightExp) const {
    if constexpr (std::is_same_v<ValueType, bool>) {
        const auto rawValueInt = static_cast<nautilus::val<int64_t>>(rawValue);
        const auto otherRawValueInt = std::dynamic_pointer_cast<ExecutableDataType<int64_t>>(rightExp)->as<int64_t>();
        const auto resultIsNull = null || rightExp->isNull();
        return ExecutableDataType<int64_t>::create(rawValueInt - otherRawValueInt, resultIsNull);
    } else if constexpr (std::is_same_v<ValueType, int8_t*>) {
        NES_NOT_IMPLEMENTED();
    } else {
        const auto otherRawValue = rightExp->as<ExecutableDataType<ValueType>>()->getRawValue();
        const auto resultIsNull = null || rightExp->isNull();
        return ExecutableDataType<ValueType>::create(rawValue - otherRawValue, resultIsNull);
    }
}

template<typename ValueType>
ExecDataType ExecutableDataType<ValueType>::operator*(const ExecDataType& rightExp) const {
    if constexpr (std::is_same_v<ValueType, bool> || std::is_same_v<ValueType, int8_t*>) {
        NES_NOT_IMPLEMENTED();
    } else {
        const auto otherRawValue = rightExp->as<ExecutableDataType<ValueType>>()->getRawValue();
        const auto resultIsNull = null || rightExp->isNull();
        return ExecutableDataType<ValueType>::create(rawValue * otherRawValue, resultIsNull);
    }
}

template<typename ValueType>
ExecDataType ExecutableDataType<ValueType>::operator/(const ExecDataType& rightExp) const {
    if constexpr (std::is_same_v<ValueType, bool> || std::is_same_v<ValueType, int8_t*>) {
        NES_NOT_IMPLEMENTED();
    } else {
        const auto otherRawValue = rightExp->as<ExecutableDataType<ValueType>>()->getRawValue();
        const auto resultIsNull = null || rightExp->isNull();
        return ExecutableDataType<ValueType>::create(rawValue / otherRawValue, resultIsNull);
    }
}

template<typename ValueType>
ExecDataType ExecutableDataType<ValueType>::operator%(const ExecDataType& rightExp) const {
    if constexpr (std::is_same_v<ValueType, bool> || std::is_same_v<ValueType, int8_t*> || std::is_same_v<ValueType, float>
                  || std::is_same_v<ValueType, double>) {
        NES_NOT_IMPLEMENTED();
    } else {
        const auto otherRawValue = rightExp->as<ExecutableDataType<ValueType>>()->getRawValue();
        const auto resultIsNull = null || rightExp->isNull();
        return ExecutableDataType<ValueType>::create(rawValue % otherRawValue, resultIsNull);
    }
}

template<typename ValueType>
ExecDataType ExecutableDataType<ValueType>::operator&(const ExecDataType& rightExp) const {
    if constexpr (std::is_same_v<ValueType, bool> || std::is_same_v<ValueType, int8_t*> || std::is_same_v<ValueType, float>
                  || std::is_same_v<ValueType, double>) {
        NES_NOT_IMPLEMENTED();
    } else {
        const auto otherRawValue = rightExp->as<ExecutableDataType<ValueType>>()->getRawValue();
        const auto resultIsNull = null || rightExp->isNull();
        return ExecutableDataType<ValueType>::create(rawValue & otherRawValue, resultIsNull);
    }
}

template<typename ValueType>
ExecDataType ExecutableDataType<ValueType>::operator|(const ExecDataType& rightExp) const {
    if constexpr (std::is_same_v<ValueType, bool> || std::is_same_v<ValueType, int8_t*> || std::is_same_v<ValueType, float>
                  || std::is_same_v<ValueType, double>) {
        NES_NOT_IMPLEMENTED();
    } else {
        const auto otherRawValue = rightExp->as<ExecutableDataType<ValueType>>()->getRawValue();
        const auto resultIsNull = null || rightExp->isNull();
        return ExecutableDataType<ValueType>::create(rawValue | otherRawValue, resultIsNull);
    }
}

template<typename ValueType>
ExecDataType ExecutableDataType<ValueType>::operator^(const ExecDataType& rightExp) const {
    if constexpr (std::is_same_v<ValueType, bool> || std::is_same_v<ValueType, int8_t*> || std::is_same_v<ValueType, float>
                  || std::is_same_v<ValueType, double>) {
        NES_NOT_IMPLEMENTED();
    } else {
        const auto otherRawValue = rightExp->as<ExecutableDataType<ValueType>>()->getRawValue();
        const auto resultIsNull = null || rightExp->isNull();
        return ExecutableDataType<ValueType>::create(rawValue ^ otherRawValue, resultIsNull);
    }
}

template<typename ValueType>
ExecDataType ExecutableDataType<ValueType>::operator<<(const ExecDataType& rightExp) const {
//    if constexpr (std::is_same_v<ValueType, bool> || std::is_same_v<ValueType, int8_t*> || std::is_same_v<ValueType, float> || std::is_same_v<ValueType, double>) {
//        NES_NOT_IMPLEMENTED();
//    } else {
//        const auto otherRawValue = rightExp->as<ExecutableDataType<ValueType>>()->getRawValue();
//        const auto resultIsNull = null || rightExp->isNull();
//        return ExecutableDataType<ValueType>::create(rawValue << otherRawValue, resultIsNull);
//    }
    NES_NOT_IMPLEMENTED();
}

template<typename ValueType>
ExecDataType ExecutableDataType<ValueType>::operator>>(const ExecDataType& rightExp) const {
    //    if constexpr (std::is_same_v<ValueType, bool> || std::is_same_v<ValueType, int8_t*> || std::is_same_v<ValueType, float> || std::is_same_v<ValueType, double>) {
    //        NES_NOT_IMPLEMENTED();
    //    } else {
    //        const auto otherRawValue = std::dynamic_pointer_cast<ExecutableDataType<ValueType>>(rightExp)->as<ValueType>;
    //        const auto resultIsNull = null || rightExp->isNull();
    //        auto ret = rawValue >> otherRawValue;
    //        return ExecutableDataType<ValueType>::create(ret, resultIsNull);
    //    }
    NES_NOT_IMPLEMENTED();
    ((void) rightExp);
}

template<typename ValueType>
ExecDataType ExecutableDataType<ValueType>::operator!() const {
    if constexpr (std::is_same_v<ValueType, bool>) {
        const auto resultIsNull = null;
        return ExecutableDataType<bool>::create(!rawValue, resultIsNull);
    } else {
        NES_NOT_IMPLEMENTED();
    }
}

ExecDataType ExecutableVariableDataType::operator&&(const ExecDataType&) const { NES_NOT_IMPLEMENTED(); }
ExecDataType ExecutableVariableDataType::operator||(const ExecDataType&) const { NES_NOT_IMPLEMENTED(); }
ExecDataType ExecutableVariableDataType::operator==(const ExecDataType&) const { NES_NOT_IMPLEMENTED(); }
ExecDataType ExecutableVariableDataType::operator!=(const ExecDataType&) const { NES_NOT_IMPLEMENTED(); }
ExecDataType ExecutableVariableDataType::operator<(const ExecDataType&) const { NES_NOT_IMPLEMENTED(); }
ExecDataType ExecutableVariableDataType::operator>(const ExecDataType&) const { NES_NOT_IMPLEMENTED(); }
ExecDataType ExecutableVariableDataType::operator<=(const ExecDataType&) const { NES_NOT_IMPLEMENTED(); }
ExecDataType ExecutableVariableDataType::operator>=(const ExecDataType&) const { NES_NOT_IMPLEMENTED(); }
ExecDataType ExecutableVariableDataType::operator+(const ExecDataType&) const { NES_NOT_IMPLEMENTED(); }
ExecDataType ExecutableVariableDataType::operator-(const ExecDataType&) const { NES_NOT_IMPLEMENTED(); }
ExecDataType ExecutableVariableDataType::operator*(const ExecDataType&) const { NES_NOT_IMPLEMENTED(); }
ExecDataType ExecutableVariableDataType::operator/(const ExecDataType&) const { NES_NOT_IMPLEMENTED(); }
ExecDataType ExecutableVariableDataType::operator%(const ExecDataType&) const { NES_NOT_IMPLEMENTED(); }
ExecDataType ExecutableVariableDataType::operator&(const ExecDataType&) const { NES_NOT_IMPLEMENTED(); }
ExecDataType ExecutableVariableDataType::operator|(const ExecDataType&) const { NES_NOT_IMPLEMENTED(); }
ExecDataType ExecutableVariableDataType::operator^(const ExecDataType&) const { NES_NOT_IMPLEMENTED(); }
ExecDataType ExecutableVariableDataType::operator<<(const ExecDataType&) const { NES_NOT_IMPLEMENTED(); }
ExecDataType ExecutableVariableDataType::operator>>(const ExecDataType&) const { NES_NOT_IMPLEMENTED(); }
ExecDataType ExecutableVariableDataType::operator!() const { NES_NOT_IMPLEMENTED(); }

template class ExecutableDataType<int8_t>;
template class ExecutableDataType<int16_t>;
template class ExecutableDataType<int32_t>;
template class ExecutableDataType<int64_t>;
template class ExecutableDataType<uint8_t>;
template class ExecutableDataType<uint16_t>;
template class ExecutableDataType<uint32_t>;
template class ExecutableDataType<uint64_t>;
template class ExecutableDataType<float>;
template class ExecutableDataType<double>;
template class ExecutableDataType<bool>;

ExecDataType readExecDataTypeFromMemRef(nautilus::val<int8_t*>& memRef, const PhysicalTypePtr& type) {
    if (type->isBasicType()) {
        auto basicType = std::static_pointer_cast<BasicPhysicalType>(type);
        switch (basicType->nativeType) {
            case BasicPhysicalType::NativeType::BOOLEAN: {
                return Nautilus::ExecutableDataType<bool>::create(
                    static_cast<nautilus::val<bool>>(*static_cast<nautilus::val<bool*>>(memRef)));
            };
            case BasicPhysicalType::NativeType::INT_8: {
                return Nautilus::ExecutableDataType<int8_t>::create(
                    static_cast<nautilus::val<int8_t>>(*static_cast<nautilus::val<int8_t*>>(memRef)));
            };
            case BasicPhysicalType::NativeType::INT_16: {
                return Nautilus::ExecutableDataType<int16_t>::create(
                    static_cast<nautilus::val<int16_t>>(*static_cast<nautilus::val<int16_t*>>(memRef)));
            };
            case BasicPhysicalType::NativeType::INT_32: {
                return Nautilus::ExecutableDataType<int32_t>::create(
                    static_cast<nautilus::val<int32_t>>(*static_cast<nautilus::val<int32_t*>>(memRef)));
            };
            case BasicPhysicalType::NativeType::INT_64: {
                return Nautilus::ExecutableDataType<int64_t>::create(
                    static_cast<nautilus::val<int64_t>>(*static_cast<nautilus::val<int64_t*>>(memRef)));
            };
            case BasicPhysicalType::NativeType::UINT_8: {
                return Nautilus::ExecutableDataType<uint8_t>::create(
                    static_cast<nautilus::val<uint8_t>>(*static_cast<nautilus::val<uint8_t*>>(memRef)));
            };
            case BasicPhysicalType::NativeType::UINT_16: {
                return Nautilus::ExecutableDataType<uint16_t>::create(
                    static_cast<nautilus::val<uint16_t>>(*static_cast<nautilus::val<uint16_t*>>(memRef)));
            };
            case BasicPhysicalType::NativeType::UINT_32: {
                return Nautilus::ExecutableDataType<uint32_t>::create(
                    static_cast<nautilus::val<uint32_t>>(*static_cast<nautilus::val<uint32_t*>>(memRef)));
            };
            case BasicPhysicalType::NativeType::UINT_64: {
                return Nautilus::ExecutableDataType<uint64_t>::create(
                    static_cast<nautilus::val<uint64_t>>(*static_cast<nautilus::val<uint64_t*>>(memRef)));
            };
            case BasicPhysicalType::NativeType::FLOAT: {
                return Nautilus::ExecutableDataType<float>::create(
                    static_cast<nautilus::val<float>>(*static_cast<nautilus::val<float*>>(memRef)));
            };
            case BasicPhysicalType::NativeType::DOUBLE: {
                return Nautilus::ExecutableDataType<double>::create(
                    static_cast<nautilus::val<double>>(*static_cast<nautilus::val<double*>>(memRef)));
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

void writeExecDataTypeToMemRef(nautilus::val<int8_t*>& memRef, const ExecDataType& execDataType) {
    if (execDataType->instanceOf<ExecutableDataType<int8_t>>()) {
        writeValueToMemRef(memRef, std::dynamic_pointer_cast<ExecutableDataType<int8_t>>(execDataType)->getRawValue(), int8_t);
    } else if (execDataType->instanceOf<ExecutableDataType<int16_t>>()) {
        writeValueToMemRef(memRef, std::dynamic_pointer_cast<ExecutableDataType<int16_t>>(execDataType)->getRawValue(), int16_t);
    } else if (execDataType->instanceOf<ExecutableDataType<int32_t>>()) {
        writeValueToMemRef(memRef, std::dynamic_pointer_cast<ExecutableDataType<int32_t>>(execDataType)->getRawValue(), int32_t);
    } else if (execDataType->instanceOf<ExecutableDataType<int64_t>>()) {
        writeValueToMemRef(memRef, std::dynamic_pointer_cast<ExecutableDataType<int64_t>>(execDataType)->getRawValue(), int64_t);
    } else if (execDataType->instanceOf<ExecutableDataType<uint8_t>>()) {
        writeValueToMemRef(memRef, std::dynamic_pointer_cast<ExecutableDataType<uint8_t>>(execDataType)->getRawValue(), uint8_t);
    } else if (execDataType->instanceOf<ExecutableDataType<uint16_t>>()) {
        writeValueToMemRef(memRef, std::dynamic_pointer_cast<ExecutableDataType<uint16_t>>(execDataType)->getRawValue(), uint16_t);
    } else if (execDataType->instanceOf<ExecutableDataType<uint32_t>>()) {
        writeValueToMemRef(memRef, std::dynamic_pointer_cast<ExecutableDataType<uint32_t>>(execDataType)->getRawValue(), uint32_t);
    } else if (execDataType->instanceOf<ExecutableDataType<uint64_t>>()) {
        writeValueToMemRef(memRef, std::dynamic_pointer_cast<ExecutableDataType<uint64_t>>(execDataType)->getRawValue(), uint64_t);
    } else if (execDataType->instanceOf<ExecutableDataType<float>>()) {
        writeValueToMemRef(memRef, std::dynamic_pointer_cast<ExecutableDataType<float>>(execDataType)->getRawValue(), float);
    } else if (execDataType->instanceOf<ExecutableDataType<double>>()) {
        writeValueToMemRef(memRef, std::dynamic_pointer_cast<ExecutableDataType<double>>(execDataType)->getRawValue(), double);
    } else if (execDataType->instanceOf<ExecutableDataType<bool>>()) {
        writeValueToMemRef(memRef, std::dynamic_pointer_cast<ExecutableDataType<bool>>(execDataType)->getRawValue(), bool);
    } else {
        NES_NOT_IMPLEMENTED();
    }
}

nautilus::val<bool> memEquals(nautilus::val<int8_t*> ptr1, nautilus::val<int8_t*> ptr2, const nautilus::val<uint64_t>& size) {
    return nautilus::invoke(std::memcmp, ptr1, ptr2, size) == nautilus::val<int>(0);
}

void memCopy(nautilus::val<int8_t*> dest, nautilus::val<int8_t*> src, const nautilus::val<uint64_t>& size) {
    nautilus::invoke(std::memcpy, dest, src, size);
}

ExecDataType Nautilus::operator==(const ExecDataType& lhs, const ExecDataType& rhs) { return *lhs == rhs; }
ExecDataType Nautilus::operator+(const ExecDataType& lhs, const ExecDataType& rhs) { return *lhs + rhs; }
ExecDataType Nautilus::operator&&(const ExecDataType& lhs, const ExecDataType& rhs) { return *lhs && rhs; }
ExecDataType Nautilus::operator||(const ExecDataType& lhs, const ExecDataType& rhs) { return *lhs || rhs; }
ExecDataType Nautilus::operator!=(const ExecDataType& lhs, const ExecDataType& rhs) { return *lhs != rhs; }
ExecDataType Nautilus::operator<(const ExecDataType& lhs, const ExecDataType& rhs) { return *lhs < rhs; }
ExecDataType Nautilus::operator>(const ExecDataType& lhs, const ExecDataType& rhs) { return *lhs > rhs; }
ExecDataType Nautilus::operator<=(const ExecDataType& lhs, const ExecDataType& rhs) { return *lhs <= rhs; }
ExecDataType Nautilus::operator>=(const ExecDataType& lhs, const ExecDataType& rhs) { return *lhs >= rhs; }
ExecDataType Nautilus::operator-(const ExecDataType& lhs, const ExecDataType& rhs) { return *lhs - rhs; }
ExecDataType Nautilus::operator*(const ExecDataType& lhs, const ExecDataType& rhs) { return *lhs * rhs; }
ExecDataType Nautilus::operator/(const ExecDataType& lhs, const ExecDataType& rhs) { return *lhs / rhs; }
ExecDataType Nautilus::operator%(const ExecDataType& lhs, const ExecDataType& rhs) { return *lhs % rhs; }
ExecDataType Nautilus::operator&(const ExecDataType& lhs, const ExecDataType& rhs) { return *lhs & rhs; }
ExecDataType Nautilus::operator|(const ExecDataType& lhs, const ExecDataType& rhs) { return *lhs | rhs; }
ExecDataType Nautilus::operator^(const ExecDataType& lhs, const ExecDataType& rhs) { return *lhs ^ rhs; }
ExecDataType Nautilus::operator<<(const ExecDataType& lhs, const ExecDataType& rhs) { return *lhs << rhs; }
ExecDataType Nautilus::operator>>(const ExecDataType& lhs, const ExecDataType& rhs) { return *lhs >> rhs; }
ExecDataType operator!(const ExecDataType& lhs) { return !*lhs; }

}// namespace NES::Nautilus

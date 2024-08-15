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
#include <nautilus/std/cstring.h>
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
        const auto otherRawValueInt = std::dynamic_pointer_cast<ExecutableDataType<int64_t>>(rightExp)->valueAsType<int64_t>();
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
        const auto otherRawValueInt = std::dynamic_pointer_cast<ExecutableDataType<int64_t>>(rightExp)->valueAsType<int64_t>();
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
    if constexpr (std::is_same_v<ValueType, bool> || std::is_same_v<ValueType, int8_t*> || std::is_same_v<ValueType, float>
                  || std::is_same_v<ValueType, double> || std::is_same_v<ValueType, uint8_t> || std::is_same_v<ValueType, int8_t>
                  || std::is_same_v<ValueType, uint16_t> || std::is_same_v<ValueType, int16_t>) {
        NES_NOT_IMPLEMENTED();
    } else {
        const auto otherRawValue = rightExp->as<ExecutableDataType<ValueType>>()->getRawValue();
        const auto resultIsNull = null || rightExp->isNull();
        auto ret = rawValue >> otherRawValue;
        return ExecutableDataType<ValueType>::create(ret, resultIsNull);
    }
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

ExecDataType ExecutableVariableDataType::operator==(const ExecDataType& rhs) const {
    if (!rhs->instanceOf<ExecutableVariableDataType>()) {
        NES_THROW_RUNTIME_ERROR("Can only compare ExecutableVariableDataType with ExecutableVariableDataType");
    }

    if (null || rhs->isNull()) {
        return ExecutableDataType<bool>::create(false, true);
    }

    if (size != rhs->as<ExecutableVariableDataType>()->size) {
        return ExecutableDataType<bool>::create(false, false);
    }

    const auto compareResult = (nautilus::memcmp(content, rhs->as<ExecutableVariableDataType>()->content, size) == 0);
    return ExecutableDataType<bool>::create(compareResult, false);
}
ExecDataType ExecutableVariableDataType::operator!=(const ExecDataType& rhs) const {
    if (!rhs->instanceOf<ExecutableVariableDataType>()) {
        NES_THROW_RUNTIME_ERROR("Can only compare ExecutableVariableDataType with ExecutableVariableDataType");
    }
    return !(*this == rhs);
}



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


ExecDataType readExecDataTypeFromMemRef(MemRefVal& memRef, const PhysicalTypePtr& type) {
    if (type->isBasicType()) {
        auto basicType = std::static_pointer_cast<BasicPhysicalType>(type);
        switch (basicType->nativeType) {
            case BasicPhysicalType::NativeType::BOOLEAN: {
                return Nautilus::ExecutableDataType<bool>::create((*static_cast<nautilus::val<bool*>>(memRef)));
            };
            case BasicPhysicalType::NativeType::INT_8: {
                return Nautilus::ExecutableDataType<int8_t>::create((*static_cast<MemRefVal>(memRef)));
            };
            case BasicPhysicalType::NativeType::INT_16: {
                return Nautilus::ExecutableDataType<int16_t>::create((*static_cast<nautilus::val<int16_t*>>(memRef)));
            };
            case BasicPhysicalType::NativeType::INT_32: {
                return Nautilus::ExecutableDataType<int32_t>::create((*static_cast<nautilus::val<int32_t*>>(memRef)));
            };
            case BasicPhysicalType::NativeType::INT_64: {
                return Nautilus::ExecutableDataType<int64_t>::create((*static_cast<nautilus::val<int64_t*>>(memRef)));
            };
            case BasicPhysicalType::NativeType::UINT_8: {
                return Nautilus::ExecutableDataType<uint8_t>::create((*static_cast<nautilus::val<uint8_t*>>(memRef)));
            };
            case BasicPhysicalType::NativeType::UINT_16: {
                return Nautilus::ExecutableDataType<uint16_t>::create((*static_cast<nautilus::val<uint16_t*>>(memRef)));
            };
            case BasicPhysicalType::NativeType::UINT_32: {
                return Nautilus::ExecutableDataType<uint32_t>::create((*static_cast<nautilus::val<uint32_t*>>(memRef)));
            };
            case BasicPhysicalType::NativeType::UINT_64: {
                return Nautilus::ExecutableDataType<uint64_t>::create((*static_cast<nautilus::val<uint64_t*>>(memRef)));
            };
            case BasicPhysicalType::NativeType::FLOAT: {
                return Nautilus::ExecutableDataType<float>::create((*static_cast<nautilus::val<float*>>(memRef)));
            };
            case BasicPhysicalType::NativeType::DOUBLE: {
                return Nautilus::ExecutableDataType<double>::create((*static_cast<nautilus::val<double*>>(memRef)));
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

void writeFixedExecDataTypeToMemRef(MemRefVal& memRef, const ExecDataType& execDataType) {
    if (execDataType->instanceOf<ExecDataInt8>()) {
        writeValueToMemRef(memRef, execDataType->as<ExecDataInt8>()->getRawValue(), int8_t);
    } else if (execDataType->instanceOf<ExecDataInt16>()) {
        writeValueToMemRef(memRef, execDataType->as<ExecDataInt16>()->getRawValue(), int16_t);
    } else if (execDataType->instanceOf<ExecDataInt32>()) {
        writeValueToMemRef(memRef, execDataType->as<ExecDataInt32>()->getRawValue(), int32_t);
    } else if (execDataType->instanceOf<ExecDataInt64>()) {
        writeValueToMemRef(memRef, execDataType->as<ExecDataInt64>()->getRawValue(), int64_t);
    } else if (execDataType->instanceOf<ExecDataUInt8>()) {
        writeValueToMemRef(memRef, execDataType->as<ExecDataUInt8>()->getRawValue(), uint8_t);
    } else if (execDataType->instanceOf<ExecDataUInt16>()) {
        writeValueToMemRef(memRef, execDataType->as<ExecDataUInt16>()->getRawValue(), uint16_t);
    } else if (execDataType->instanceOf<ExecDataUInt32>()) {
        writeValueToMemRef(memRef, execDataType->as<ExecDataUInt32>()->getRawValue(), uint32_t);
    } else if (execDataType->instanceOf<ExecDataUInt64>()) {
        writeValueToMemRef(memRef, execDataType->as<ExecDataUInt64>()->getRawValue(), uint64_t);
    } else if (execDataType->instanceOf<ExecDataFloat>()) {
        writeValueToMemRef(memRef, execDataType->as<ExecDataFloat>()->getRawValue(), float);
    } else if (execDataType->instanceOf<ExecDataDouble>()) {
        writeValueToMemRef(memRef, execDataType->as<ExecDataDouble>()->getRawValue(), double);
    } else if (execDataType->instanceOf<ExecDataBoolean>()) {
        writeValueToMemRef(memRef, execDataType->as<ExecDataBoolean>()->getRawValue(), bool);
    } else {
        NES_NOT_IMPLEMENTED();
    }
}

ExecDataType Nautilus::operator==(const ExecDataType& lhs, const ExecDataType& rhs) {
    const auto [lhsCasted, rhsCasted] = castToSameType(lhs, rhs);
    return *lhsCasted == rhsCasted;
}
ExecDataType Nautilus::operator+(const ExecDataType& lhs, const ExecDataType& rhs) {
    const auto [lhsCasted, rhsCasted] = castToSameType(lhs, rhs);
    return *lhsCasted + rhsCasted;
}
ExecDataType Nautilus::operator&&(const ExecDataType& lhs, const ExecDataType& rhs) {
    const auto [lhsCasted, rhsCasted] = castToSameType(lhs, rhs);
    return *lhsCasted && rhsCasted;
}
ExecDataType Nautilus::operator||(const ExecDataType& lhs, const ExecDataType& rhs) {
    const auto [lhsCasted, rhsCasted] = castToSameType(lhs, rhs);
    return *lhsCasted || rhsCasted;
}
ExecDataType Nautilus::operator!=(const ExecDataType& lhs, const ExecDataType& rhs) {
    const auto [lhsCasted, rhsCasted] = castToSameType(lhs, rhs);
    return *lhsCasted != rhsCasted;
}

ExecDataType Nautilus::operator<(const ExecDataType& lhs, const ExecDataType& rhs) {
    const auto [lhsCasted, rhsCasted] = castToSameType(lhs, rhs);
    return *lhsCasted < rhsCasted;
}

ExecDataType Nautilus::operator>(const ExecDataType& lhs, const ExecDataType& rhs) {
    const auto [lhsCasted, rhsCasted] = castToSameType(lhs, rhs);
    return *lhsCasted > rhsCasted;
}
ExecDataType Nautilus::operator<=(const ExecDataType& lhs, const ExecDataType& rhs) {
    const auto [lhsCasted, rhsCasted] = castToSameType(lhs, rhs);
    return *lhsCasted <= rhsCasted;
}
ExecDataType Nautilus::operator>=(const ExecDataType& lhs, const ExecDataType& rhs) {
    const auto [lhsCasted, rhsCasted] = castToSameType(lhs, rhs);
    return *lhsCasted >= rhsCasted;
}
ExecDataType Nautilus::operator-(const ExecDataType& lhs, const ExecDataType& rhs) {
    const auto [lhsCasted, rhsCasted] = castToSameType(lhs, rhs);
    return *lhsCasted - rhsCasted;
}
ExecDataType Nautilus::operator*(const ExecDataType& lhs, const ExecDataType& rhs) {
    const auto [lhsCasted, rhsCasted] = castToSameType(lhs, rhs);
    return *lhsCasted * rhsCasted;
}
ExecDataType Nautilus::operator/(const ExecDataType& lhs, const ExecDataType& rhs) {
    const auto [lhsCasted, rhsCasted] = castToSameType(lhs, rhs);
    return *lhsCasted / rhsCasted;
}
ExecDataType Nautilus::operator%(const ExecDataType& lhs, const ExecDataType& rhs) {
    const auto [lhsCasted, rhsCasted] = castToSameType(lhs, rhs);
    return *lhsCasted % rhsCasted;
}
ExecDataType Nautilus::operator&(const ExecDataType& lhs, const ExecDataType& rhs) {
    const auto [lhsCasted, rhsCasted] = castToSameType(lhs, rhs);
    return *lhsCasted & rhsCasted;
}
ExecDataType Nautilus::operator|(const ExecDataType& lhs, const ExecDataType& rhs) {
    const auto [lhsCasted, rhsCasted] = castToSameType(lhs, rhs);
    return *lhsCasted | rhsCasted;
}
ExecDataType Nautilus::operator^(const ExecDataType& lhs, const ExecDataType& rhs) {
    const auto [lhsCasted, rhsCasted] = castToSameType(lhs, rhs);
    return *lhsCasted ^ rhsCasted;
}
ExecDataType Nautilus::operator<<(const ExecDataType& lhs, const ExecDataType& rhs) {
    const auto [lhsCasted, rhsCasted] = castToSameType(lhs, rhs);

    /// We can assume that the lhsCasted and rhsCasted are the same type here
    /// For some reason anything below a int gets casted to a int, therefore we need to cast it here to a int32_t
    if (lhsCasted->instanceOf<ExecDataUInt8>() || lhsCasted->instanceOf<ExecDataInt8>() || lhsCasted->instanceOf<ExecDataUInt16>()
        || lhsCasted->instanceOf<ExecDataInt16>()) {
        const auto lhsCastedInt = castTo<int32_t>(lhsCasted);
        const auto rhsCastedInt = castTo<int32_t>(rhsCasted);
        return *lhsCastedInt << rhsCastedInt;
    }
    return *lhsCasted << rhsCasted;
}

ExecDataType Nautilus::operator>>(const ExecDataType& lhs, const ExecDataType& rhs) {
    const auto [lhsCasted, rhsCasted] = castToSameType(lhs, rhs);

    /// We can assume that the lhsCasted and rhsCasted are the same type here
    /// For some reason anything below a int gets casted to a int, therefore we need to cast it here to a int32_t
    if (lhsCasted->instanceOf<ExecDataUInt8>() || lhsCasted->instanceOf<ExecDataInt8>() || lhsCasted->instanceOf<ExecDataUInt16>()
        || lhsCasted->instanceOf<ExecDataInt16>()) {
        const auto lhsCastedInt = castTo<int32_t>(lhsCasted);
        const auto rhsCastedInt = castTo<int32_t>(rhsCasted);
        return *lhsCastedInt >> rhsCastedInt;
    }

    return *lhsCasted >> rhsCasted;
}
ExecDataType operator!(const ExecDataType& lhs) { return !*lhs; }

std::pair<ExecDataType, ExecDataType> castToSameType(const ExecDataType& lhs, const ExecDataType& rhs) {
    /// If lhs and rhs are the same type, we can call the operator directly without any conversion
    if (typeid(*lhs) == typeid(*rhs)) {
        return {lhs, rhs};
    }

    /// If lhs and rhs are not the same type, we need to convert one of them to the other type
    /// We will convert the one with the smaller type to the larger type, e.g.,
    /// double op X --> double op double
    /// float op Integer --> float op float

    /// double op X --> double op double
    if (lhs->instanceOf<ExecDataDouble>()) {
        const auto rhsCasted = castTo<double>(rhs);
        return {lhs, rhsCasted};
    }
    if (rhs->instanceOf<ExecDataDouble>()) {
        const auto lhsCasted = castTo<double>(lhs);
        return {lhsCasted, rhs};
    }

    /// float op integer --> float op float
    if (lhs->instanceOf<ExecDataFloat>()) {
        const auto rhsCasted = castTo<float>(rhs);
        return {lhs, rhsCasted};
    }
    if (rhs->instanceOf<ExecDataFloat>()) {
        const auto lhsCasted = castTo<float>(lhs);
        return {lhsCasted, rhs};
    }

    /// uint64_t op X --> uint64_t op uint64_t
    /// (u)intX op (u)intY --> (u)intX op (u)intX (assuming X has a higher rank, e.g, 64 vs. 32 bit)
    /// We check here from the highest rank, 64 bit, down to the lowest rank.
    /// Additionally, we check first for unsigned and then for signed, as unsigned and signed with the same rank get casted to unsigned
    /// For more information see https://en.cppreference.com/w/cpp/language/usual_arithmetic_conversions
    if (lhs->instanceOf<ExecDataUInt64>()) {
        const auto rhsCasted = castTo<uint64_t>(rhs);
        return {lhs, rhsCasted};
    }
    if (rhs->instanceOf<ExecDataUInt64>()) {
        const auto lhsCasted = castTo<uint64_t>(lhs);
        return {lhsCasted, rhs};
    }
    if (lhs->instanceOf<ExecDataInt64>()) {
        const auto rhsCasted = castTo<int64_t>(rhs);
        return {lhs, rhsCasted};
    }
    if (rhs->instanceOf<ExecDataInt64>()) {
        const auto lhsCasted = castTo<int64_t>(lhs);
        return {lhsCasted, rhs};
    }
    if (lhs->instanceOf<ExecDataUInt32>()) {
        const auto rhsCasted = castTo<uint32_t>(rhs);
        return {lhs, rhsCasted};
    }
    if (rhs->instanceOf<ExecDataUInt32>()) {
        const auto lhsCasted = castTo<uint32_t>(lhs);
        return {lhsCasted, rhs};
    }
    if (lhs->instanceOf<ExecDataInt32>()) {
        const auto rhsCasted = castTo<int32_t>(rhs);
        return {lhs, rhsCasted};
    }
    if (rhs->instanceOf<ExecDataInt32>()) {
        const auto lhsCasted = castTo<int32_t>(lhs);
        return {lhsCasted, rhs};
    }
    if (lhs->instanceOf<ExecDataUInt16>()) {
        const auto rhsCasted = castTo<uint16_t>(rhs);
        return {lhs, rhsCasted};
    }
    if (rhs->instanceOf<ExecDataUInt16>()) {
        const auto lhsCasted = castTo<uint16_t>(lhs);
        return {lhsCasted, rhs};
    }
    if (lhs->instanceOf<ExecDataInt16>()) {
        const auto rhsCasted = castTo<int16_t>(rhs);
        return {lhs, rhsCasted};
    }
    if (rhs->instanceOf<ExecDataInt16>()) {
        const auto lhsCasted = castTo<int16_t>(lhs);
        return {lhsCasted, rhs};
    }
    if (lhs->instanceOf<ExecDataUInt8>()) {
        const auto rhsCasted = castTo<uint8_t>(rhs);
        return {lhs, rhsCasted};
    }
    if (rhs->instanceOf<ExecDataUInt8>()) {
        const auto lhsCasted = castTo<uint8_t>(lhs);
        return {lhsCasted, rhs};
    }

    NES_NOT_IMPLEMENTED();
}

}// namespace NES::Nautilus

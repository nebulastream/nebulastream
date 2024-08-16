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

#include <Nautilus/DataTypes/FixedSizeExecutableDataType.hpp>
#include <Nautilus/DataTypes/Operations/ExecutableDataTypeOperations.hpp>
#include <Util/Logger/Logger.hpp>
#include <nautilus/std/cstring.h>
#include <nautilus/val.hpp>
#include <nautilus/val_ptr.hpp>
#include <utility>

namespace NES::Nautilus {
template class FixedSizeExecutableDataType<int8_t>;
template class FixedSizeExecutableDataType<int16_t>;
template class FixedSizeExecutableDataType<int32_t>;
template class FixedSizeExecutableDataType<int64_t>;
template class FixedSizeExecutableDataType<uint8_t>;
template class FixedSizeExecutableDataType<uint16_t>;
template class FixedSizeExecutableDataType<uint32_t>;
template class FixedSizeExecutableDataType<uint64_t>;
template class FixedSizeExecutableDataType<float>;
template class FixedSizeExecutableDataType<double>;
template class FixedSizeExecutableDataType<bool>;

// Define all operations on ExecutableDataType
template<typename ValueType>
ExecDataType FixedSizeExecutableDataType<ValueType>::operator&&(const ExecDataType& rightExp) const {
    const auto resultIsNull = null || rightExp->isNull();
    const auto rawValueBool = static_cast<nautilus::val<bool>>(rawValue);
    const auto otherRawValueBool =
        static_cast<nautilus::val<bool>>(std::dynamic_pointer_cast<FixedSizeExecutableDataType<ValueType>>(rightExp)->rawValue);
    return FixedSizeExecutableDataType<bool>::create(rawValueBool && otherRawValueBool, resultIsNull);
}

template<typename ValueType>
ExecDataType FixedSizeExecutableDataType<ValueType>::operator||(const ExecDataType& rightExp) const {
    const auto rawValueBool = static_cast<nautilus::val<bool>>(rawValue);
    const auto otherRawValueBool = static_cast<nautilus::val<bool>>(
        std::dynamic_pointer_cast<FixedSizeExecutableDataType<ValueType>>(rightExp)->rawValue);
    const auto resultIsNull = null || rightExp->isNull();
    return FixedSizeExecutableDataType<bool>::create(rawValueBool || otherRawValueBool, resultIsNull);
}

template<typename ValueType>
ExecDataType FixedSizeExecutableDataType<ValueType>::operator==(const ExecDataType& rightExp) const {
    const auto otherRawValue = rightExp->as<FixedSizeExecutableDataType<ValueType>>()->template valueAsType<ValueType>();
    const auto resultIsNull = null || rightExp->isNull();
    const auto result = rawValue == otherRawValue;
    return FixedSizeExecutableDataType<bool>::create(result, resultIsNull);
}

template<typename ValueType>
ExecDataType FixedSizeExecutableDataType<ValueType>::operator!=(const ExecDataType& rightExp) const {
    const auto otherRawValue = rightExp->as<FixedSizeExecutableDataType<ValueType>>()->template valueAsType<ValueType>();
    const auto resultIsNull = null || rightExp->isNull();
    return FixedSizeExecutableDataType<bool>::create(rawValue != otherRawValue, resultIsNull);
}

template<typename ValueType>
ExecDataType FixedSizeExecutableDataType<ValueType>::operator<(const ExecDataType& rightExp) const {
    const auto otherRawValue = rightExp->as<FixedSizeExecutableDataType<ValueType>>()->template valueAsType<ValueType>();
    const auto resultIsNull = null || rightExp->isNull();
    return FixedSizeExecutableDataType<bool>::create(rawValue < otherRawValue, resultIsNull);
}

template<typename ValueType>
ExecDataType FixedSizeExecutableDataType<ValueType>::operator>(const ExecDataType& rightExp) const {
    const auto otherRawValue = rightExp->as<FixedSizeExecutableDataType<ValueType>>()->template valueAsType<ValueType>();
    const auto resultIsNull = null || rightExp->isNull();
    return FixedSizeExecutableDataType<bool>::create(rawValue > otherRawValue, resultIsNull);
}

template<typename ValueType>
ExecDataType FixedSizeExecutableDataType<ValueType>::operator<=(const ExecDataType& rightExp) const {
    const auto otherRawValue = rightExp->as<FixedSizeExecutableDataType<ValueType>>()->template valueAsType<ValueType>();
    const auto resultIsNull = null || rightExp->isNull();
    return FixedSizeExecutableDataType<bool>::create(rawValue <= otherRawValue, resultIsNull);
}

template<typename ValueType>
ExecDataType FixedSizeExecutableDataType<ValueType>::operator>=(const ExecDataType& rightExp) const {
    const auto otherRawValue = rightExp->as<FixedSizeExecutableDataType<ValueType>>()->template valueAsType<ValueType>();
    const auto resultIsNull = null || rightExp->isNull();
    return FixedSizeExecutableDataType<bool>::create(rawValue >= otherRawValue, resultIsNull);
}

template<typename ValueType>
ExecDataType FixedSizeExecutableDataType<ValueType>::operator+(const ExecDataType& rightExp) const {
    if constexpr (std::is_same_v<ValueType, bool>) {
        const auto rawValueInt = static_cast<nautilus::val<int64_t>>(rawValue);
        const auto otherRawValueInt = castAndLoadValue<int64_t>(rightExp);
        const auto resultIsNull = null || rightExp->isNull();
        return FixedSizeExecutableDataType<int64_t>::create(rawValueInt + otherRawValueInt, resultIsNull);
    } else {
        const auto otherRawValue = rightExp->as<FixedSizeExecutableDataType<ValueType>>()->template valueAsType<ValueType>();
        const auto resultIsNull = null || rightExp->isNull();
        return FixedSizeExecutableDataType<ValueType>::create(rawValue + otherRawValue, resultIsNull);
    }
}

template<typename ValueType>
ExecDataType FixedSizeExecutableDataType<ValueType>::operator-(const ExecDataType& rightExp) const {
    if constexpr (std::is_same_v<ValueType, bool>) {
        const auto rawValueInt = static_cast<nautilus::val<int64_t>>(rawValue);
        const auto otherRawValueInt = castAndLoadValue<int64_t>(rightExp);
        const auto resultIsNull = null || rightExp->isNull();
        return FixedSizeExecutableDataType<int64_t>::create(rawValueInt - otherRawValueInt, resultIsNull);
    } else if constexpr (std::is_same_v<ValueType, int8_t*>) {
        NES_NOT_IMPLEMENTED();
    } else {
        const auto otherRawValue = rightExp->as<FixedSizeExecutableDataType<ValueType>>()->template valueAsType<ValueType>();
        const auto resultIsNull = null || rightExp->isNull();
        return FixedSizeExecutableDataType<ValueType>::create(rawValue - otherRawValue, resultIsNull);
    }
}

template<typename ValueType>
ExecDataType FixedSizeExecutableDataType<ValueType>::operator*(const ExecDataType& rightExp) const {
    if constexpr (std::is_same_v<ValueType, bool> || std::is_same_v<ValueType, int8_t*>) {
        NES_NOT_IMPLEMENTED();
    } else {
        const auto otherRawValue = rightExp->as<FixedSizeExecutableDataType<ValueType>>()->template valueAsType<ValueType>();
        const auto resultIsNull = null || rightExp->isNull();
        return FixedSizeExecutableDataType<ValueType>::create(rawValue * otherRawValue, resultIsNull);
    }
}

template<typename ValueType>
ExecDataType FixedSizeExecutableDataType<ValueType>::operator/(const ExecDataType& rightExp) const {
    if constexpr (std::is_same_v<ValueType, bool> || std::is_same_v<ValueType, int8_t*>) {
        NES_NOT_IMPLEMENTED();
    } else {
        const auto otherRawValue = rightExp->as<FixedSizeExecutableDataType<ValueType>>()->template valueAsType<ValueType>();
        const auto resultIsNull = null || rightExp->isNull();
        return FixedSizeExecutableDataType<ValueType>::create(rawValue / otherRawValue, resultIsNull);
    }
}

template<typename ValueType>
ExecDataType FixedSizeExecutableDataType<ValueType>::operator%(const ExecDataType& rightExp) const {
    if constexpr (std::is_same_v<ValueType, bool> || std::is_same_v<ValueType, int8_t*> || std::is_same_v<ValueType, float>
                  || std::is_same_v<ValueType, double>) {
        NES_NOT_IMPLEMENTED();
    } else {
        const auto otherRawValue = rightExp->as<FixedSizeExecutableDataType<ValueType>>()->template valueAsType<ValueType>();
        const auto resultIsNull = null || rightExp->isNull();
        return FixedSizeExecutableDataType<ValueType>::create(rawValue % otherRawValue, resultIsNull);
    }
}

template<typename ValueType>
ExecDataType FixedSizeExecutableDataType<ValueType>::operator&(const ExecDataType& rightExp) const {
    if constexpr (std::is_same_v<ValueType, bool> || std::is_same_v<ValueType, int8_t*> || std::is_same_v<ValueType, float>
                  || std::is_same_v<ValueType, double>) {
        NES_NOT_IMPLEMENTED();
    } else {
        const auto otherRawValue = rightExp->as<FixedSizeExecutableDataType<ValueType>>()->template valueAsType<ValueType>();
        const auto resultIsNull = null || rightExp->isNull();
        return FixedSizeExecutableDataType<ValueType>::create(rawValue & otherRawValue, resultIsNull);
    }
}

template<typename ValueType>
ExecDataType FixedSizeExecutableDataType<ValueType>::operator|(const ExecDataType& rightExp) const {
    if constexpr (std::is_same_v<ValueType, bool> || std::is_same_v<ValueType, int8_t*> || std::is_same_v<ValueType, float>
                  || std::is_same_v<ValueType, double>) {
        NES_NOT_IMPLEMENTED();
    } else {
        const auto otherRawValue = rightExp->as<FixedSizeExecutableDataType<ValueType>>()->template valueAsType<ValueType>();
        const auto resultIsNull = null || rightExp->isNull();
        return FixedSizeExecutableDataType<ValueType>::create(rawValue | otherRawValue, resultIsNull);
    }
}

template<typename ValueType>
ExecDataType FixedSizeExecutableDataType<ValueType>::operator^(const ExecDataType& rightExp) const {
    if constexpr (std::is_same_v<ValueType, bool> || std::is_same_v<ValueType, int8_t*> || std::is_same_v<ValueType, float>
                  || std::is_same_v<ValueType, double>) {
        NES_NOT_IMPLEMENTED();
    } else {
        const auto otherRawValue = rightExp->as<FixedSizeExecutableDataType<ValueType>>()->template valueAsType<ValueType>();
        const auto resultIsNull = null || rightExp->isNull();
        return FixedSizeExecutableDataType<ValueType>::create(rawValue ^ otherRawValue, resultIsNull);
    }
}

template<typename ValueType>
ExecDataType FixedSizeExecutableDataType<ValueType>::operator<<(const ExecDataType& rightExp) const {
    if constexpr (std::is_same_v<ValueType, bool> || std::is_same_v<ValueType, int8_t*> || std::is_same_v<ValueType, float>
                  || std::is_same_v<ValueType, double> || std::is_same_v<ValueType, uint8_t> || std::is_same_v<ValueType, int8_t>
                  || std::is_same_v<ValueType, uint16_t> || std::is_same_v<ValueType, int16_t>) {
        NES_NOT_IMPLEMENTED();
    } else {
        const auto otherRawValue = rightExp->as<FixedSizeExecutableDataType<ValueType>>()->template valueAsType<ValueType>();
        const auto resultIsNull = null || rightExp->isNull();
        auto ret = rawValue << otherRawValue;
        return FixedSizeExecutableDataType<ValueType>::create(ret, resultIsNull);
    }
}

template<typename ValueType>
ExecDataType FixedSizeExecutableDataType<ValueType>::operator>>(const ExecDataType& rightExp) const {
    if constexpr (std::is_same_v<ValueType, bool> || std::is_same_v<ValueType, int8_t*> || std::is_same_v<ValueType, float>
                  || std::is_same_v<ValueType, double> || std::is_same_v<ValueType, uint8_t> || std::is_same_v<ValueType, int8_t>
                  || std::is_same_v<ValueType, uint16_t> || std::is_same_v<ValueType, int16_t>) {
        NES_NOT_IMPLEMENTED();
    } else {
        const auto otherRawValue = rightExp->as<FixedSizeExecutableDataType<ValueType>>()->template valueAsType<ValueType>();
        const auto resultIsNull = null || rightExp->isNull();
        auto ret = rawValue >> otherRawValue;
        return FixedSizeExecutableDataType<ValueType>::create(ret, resultIsNull);
    }
}

template<typename ValueType>
ExecDataType FixedSizeExecutableDataType<ValueType>::operator!() const {
    if constexpr (std::is_same_v<ValueType, bool>) {
        const auto resultIsNull = null;
        return FixedSizeExecutableDataType<bool>::create(!rawValue, resultIsNull);
    } else {
        NES_NOT_IMPLEMENTED();
    }
}

template<typename ValueType>
std::string FixedSizeExecutableDataType<ValueType>::toString() const {
    if (null) {
        return "NULL";
    } else {
        return rawValue.toString();
    }
}

template<typename ValueType>
nautilus::val<bool> FixedSizeExecutableDataType<ValueType>::isBool() const {
    return rawValue == true;
}

template<typename ValueType>
void FixedSizeExecutableDataType<ValueType>::writeToMemRefVal(const MemRefVal& memRef) {
    *static_cast<nautilus::val<ValueType*>>(memRef) = rawValue;
}

ExecDataType readExecDataTypeFromMemRef(MemRefVal& memRef, const PhysicalTypePtr& type) {
    if (type->isBasicType()) {
        auto basicType = std::static_pointer_cast<BasicPhysicalType>(type);
        switch (basicType->nativeType) {
            case BasicPhysicalType::NativeType::BOOLEAN: {
                return Nautilus::FixedSizeExecutableDataType<bool>::create((*static_cast<nautilus::val<bool*>>(memRef)));
            };
            case BasicPhysicalType::NativeType::INT_8: {
                return Nautilus::FixedSizeExecutableDataType<int8_t>::create((*static_cast<MemRefVal>(memRef)));
            };
            case BasicPhysicalType::NativeType::INT_16: {
                return Nautilus::FixedSizeExecutableDataType<int16_t>::create((*static_cast<nautilus::val<int16_t*>>(memRef)));
            };
            case BasicPhysicalType::NativeType::INT_32: {
                return Nautilus::FixedSizeExecutableDataType<int32_t>::create((*static_cast<nautilus::val<int32_t*>>(memRef)));
            };
            case BasicPhysicalType::NativeType::INT_64: {
                return Nautilus::FixedSizeExecutableDataType<int64_t>::create((*static_cast<nautilus::val<int64_t*>>(memRef)));
            };
            case BasicPhysicalType::NativeType::UINT_8: {
                return Nautilus::FixedSizeExecutableDataType<uint8_t>::create((*static_cast<nautilus::val<uint8_t*>>(memRef)));
            };
            case BasicPhysicalType::NativeType::UINT_16: {
                return Nautilus::FixedSizeExecutableDataType<uint16_t>::create((*static_cast<nautilus::val<uint16_t*>>(memRef)));
            };
            case BasicPhysicalType::NativeType::UINT_32: {
                return Nautilus::FixedSizeExecutableDataType<uint32_t>::create((*static_cast<nautilus::val<uint32_t*>>(memRef)));
            };
            case BasicPhysicalType::NativeType::UINT_64: {
                return Nautilus::FixedSizeExecutableDataType<uint64_t>::create((*static_cast<nautilus::val<uint64_t*>>(memRef)));
            };
            case BasicPhysicalType::NativeType::FLOAT: {
                return Nautilus::FixedSizeExecutableDataType<float>::create((*static_cast<nautilus::val<float*>>(memRef)));
            };
            case BasicPhysicalType::NativeType::DOUBLE: {
                return Nautilus::FixedSizeExecutableDataType<double>::create((*static_cast<nautilus::val<double*>>(memRef)));
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
    if (lhs->instanceOf<FixedSizeExecutableDataType<Double>>()) {
        const auto rhsCasted = castTo<double>(rhs);
        return {lhs, rhsCasted};
    }
    if (rhs->instanceOf<FixedSizeExecutableDataType<Double>>()) {
        const auto lhsCasted = castTo<double>(lhs);
        return {lhsCasted, rhs};
    }

    /// float op integer --> float op float
    if (lhs->instanceOf<FixedSizeExecutableDataType<Float>>()) {
        const auto rhsCasted = castTo<float>(rhs);
        return {lhs, rhsCasted};
    }
    if (rhs->instanceOf<FixedSizeExecutableDataType<Float>>()) {
        const auto lhsCasted = castTo<float>(lhs);
        return {lhsCasted, rhs};
    }

    /// uint64_t op X --> uint64_t op uint64_t
    /// (u)intX op (u)intY --> (u)intX op (u)intX (assuming X has a higher rank, e.g, 64 vs. 32 bit)
    /// We check here from the highest rank, 64 bit, down to the lowest rank.
    /// Additionally, we check first for unsigned and then for signed, as unsigned and signed with the same rank get casted to unsigned
    /// For more information see https://en.cppreference.com/w/cpp/language/usual_arithmetic_conversions
    if (lhs->instanceOf<FixedSizeExecutableDataType<UInt64>>()) {
        const auto rhsCasted = castTo<uint64_t>(rhs);
        return {lhs, rhsCasted};
    }
    if (rhs->instanceOf<FixedSizeExecutableDataType<UInt64>>()) {
        const auto lhsCasted = castTo<uint64_t>(lhs);
        return {lhsCasted, rhs};
    }
    if (lhs->instanceOf<FixedSizeExecutableDataType<Int64>>()) {
        const auto rhsCasted = castTo<int64_t>(rhs);
        return {lhs, rhsCasted};
    }
    if (rhs->instanceOf<FixedSizeExecutableDataType<Int64>>()) {
        const auto lhsCasted = castTo<int64_t>(lhs);
        return {lhsCasted, rhs};
    }
    if (lhs->instanceOf<FixedSizeExecutableDataType<UInt32>>()) {
        const auto rhsCasted = castTo<uint32_t>(rhs);
        return {lhs, rhsCasted};
    }
    if (rhs->instanceOf<FixedSizeExecutableDataType<UInt32>>()) {
        const auto lhsCasted = castTo<uint32_t>(lhs);
        return {lhsCasted, rhs};
    }
    if (lhs->instanceOf<FixedSizeExecutableDataType<Int32>>()) {
        const auto rhsCasted = castTo<int32_t>(rhs);
        return {lhs, rhsCasted};
    }
    if (rhs->instanceOf<FixedSizeExecutableDataType<Int32>>()) {
        const auto lhsCasted = castTo<int32_t>(lhs);
        return {lhsCasted, rhs};
    }
    if (lhs->instanceOf<FixedSizeExecutableDataType<UInt16>>()) {
        const auto rhsCasted = castTo<uint16_t>(rhs);
        return {lhs, rhsCasted};
    }
    if (rhs->instanceOf<FixedSizeExecutableDataType<UInt16>>()) {
        const auto lhsCasted = castTo<uint16_t>(lhs);
        return {lhsCasted, rhs};
    }
    if (lhs->instanceOf<FixedSizeExecutableDataType<Int16>>()) {
        const auto rhsCasted = castTo<int16_t>(rhs);
        return {lhs, rhsCasted};
    }
    if (rhs->instanceOf<FixedSizeExecutableDataType<Int16>>()) {
        const auto lhsCasted = castTo<int16_t>(lhs);
        return {lhsCasted, rhs};
    }
    if (lhs->instanceOf<FixedSizeExecutableDataType<UInt8>>()) {
        const auto rhsCasted = castTo<uint8_t>(rhs);
        return {lhs, rhsCasted};
    }
    if (rhs->instanceOf<FixedSizeExecutableDataType<UInt8>>()) {
        const auto lhsCasted = castTo<uint8_t>(lhs);
        return {lhsCasted, rhs};
    }

    NES_NOT_IMPLEMENTED();
}

}// namespace NES::Nautilus

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
    const auto otherRawValue = std::dynamic_pointer_cast<ExecutableDataType<ValueType>>(rightExp)->rawValue;
    const auto resultIsNull = null || rightExp->isNull();
    const auto result = rawValue == otherRawValue;
    return ExecutableDataType<bool>::create(result, resultIsNull);
}

template<typename ValueType>
ExecDataType ExecutableDataType<ValueType>::operator!=(const ExecDataType& rightExp) const {
    const auto otherRawValue = std::dynamic_pointer_cast<ExecutableDataType<ValueType>>(rightExp)->rawValue;
    const auto resultIsNull = null || rightExp->isNull();
    return ExecutableDataType<bool>::create(rawValue != otherRawValue, resultIsNull);
}

template<typename ValueType>
ExecDataType ExecutableDataType<ValueType>::operator<(const ExecDataType& rightExp) const {
    const auto otherRawValue = std::dynamic_pointer_cast<ExecutableDataType<ValueType>>(rightExp)->rawValue;
    const auto resultIsNull = null || rightExp->isNull();
    return ExecutableDataType<bool>::create(rawValue < otherRawValue, resultIsNull);
}

template<typename ValueType>
ExecDataType ExecutableDataType<ValueType>::operator>(const ExecDataType& rightExp) const {
    const auto otherRawValue = std::dynamic_pointer_cast<ExecutableDataType<ValueType>>(rightExp)->rawValue;
    const auto resultIsNull = null || rightExp->isNull();
    return ExecutableDataType<bool>::create(rawValue > otherRawValue, resultIsNull);
}

template<typename ValueType>
ExecDataType ExecutableDataType<ValueType>::operator<=(const ExecDataType& rightExp) const {
    const auto otherRawValue = std::dynamic_pointer_cast<ExecutableDataType<ValueType>>(rightExp)->rawValue;
    const auto resultIsNull = null || rightExp->isNull();
    return ExecutableDataType<bool>::create(rawValue <= otherRawValue, resultIsNull);
}

template<typename ValueType>
ExecDataType ExecutableDataType<ValueType>::operator>=(const ExecDataType& rightExp) const {
    const auto otherRawValue = std::dynamic_pointer_cast<ExecutableDataType<ValueType>>(rightExp)->rawValue;
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
        const auto otherRawValue = std::dynamic_pointer_cast<ExecutableDataType<ValueType>>(rightExp)->rawValue;
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
        const auto otherRawValue = std::dynamic_pointer_cast<ExecutableDataType<ValueType>>(rightExp)->rawValue;
        const auto resultIsNull = null || rightExp->isNull();
        return ExecutableDataType<ValueType>::create(rawValue - otherRawValue, resultIsNull);
    }
}

template<typename ValueType>
ExecDataType ExecutableDataType<ValueType>::operator*(const ExecDataType& rightExp) const {
    if constexpr (std::is_same_v<ValueType, bool> || std::is_same_v<ValueType, int8_t*>) {
        NES_NOT_IMPLEMENTED();
    } else {
        const auto otherRawValue = std::dynamic_pointer_cast<ExecutableDataType<ValueType>>(rightExp)->rawValue;
        const auto resultIsNull = null || rightExp->isNull();
        return ExecutableDataType<ValueType>::create(rawValue * otherRawValue, resultIsNull);
    }
}

template<typename ValueType>
ExecDataType ExecutableDataType<ValueType>::operator/(const ExecDataType& rightExp) const {
    if constexpr (std::is_same_v<ValueType, bool> || std::is_same_v<ValueType, int8_t*>) {
        NES_NOT_IMPLEMENTED();
    } else {
        const auto otherRawValue = std::dynamic_pointer_cast<ExecutableDataType<ValueType>>(rightExp)->rawValue;
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
        const auto otherRawValue = std::dynamic_pointer_cast<ExecutableDataType<ValueType>>(rightExp)->rawValue;
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
        const auto otherRawValue = std::dynamic_pointer_cast<ExecutableDataType<ValueType>>(rightExp)->rawValue;
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
        const auto otherRawValue = std::dynamic_pointer_cast<ExecutableDataType<ValueType>>(rightExp)->rawValue;
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
        const auto otherRawValue = std::dynamic_pointer_cast<ExecutableDataType<ValueType>>(rightExp)->rawValue;
        const auto resultIsNull = null || rightExp->isNull();
        return ExecutableDataType<ValueType>::create(rawValue ^ otherRawValue, resultIsNull);
    }
}

template<typename ValueType>
ExecDataType ExecutableDataType<ValueType>::operator<<(const ExecDataType& rightExp) const {
    //    if constexpr (std::is_same_v<ValueType, bool> || std::is_same_v<ValueType, int8_t*> || std::is_same_v<ValueType, float> || std::is_same_v<ValueType, double>) {
    //        NES_NOT_IMPLEMENTED();
    //    } else {
    //        const auto otherRawValue = std::dynamic_pointer_cast<ExecutableDataType<ValueType>>(rightExp)->rawValue;
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

template<typename LHS, typename RHS>
nautilus::val<bool> isEqual(ExecutableDataType<LHS> lhs, nautilus::val<RHS> rhs) {
    const auto lhsRawValue = lhs.getRawValue();
    return lhsRawValue == rhs;
}
nautilus::val<bool> operator==(ExecDataType lhs, nautilus::val<bool> rhs) {
    const auto lhsAsBool = std::dynamic_pointer_cast<ExecutableDataType<bool>>(lhs);
    return isEqual<bool, bool>(*lhsAsBool, rhs);
}
nautilus::val<bool> operator==(ExecDataType lhs, bool rhs) {
    return operator==(lhs, nautilus::val<bool>(rhs));
}

} /// namespace NES::Nautilus
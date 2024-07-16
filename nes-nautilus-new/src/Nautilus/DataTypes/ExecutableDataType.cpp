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
#include <nautilus/val.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Nautilus {

// Define all operations on ExecutableDataType
template<typename ValueType>
ExecDataType ExecutableDataType<ValueType>::operator&&(const AbstractDataType& rightExp) const {
    const auto rawValueBool = static_cast<nautilus::val<bool>>(rawValue);
    const auto otherRawValueBool = static_cast<nautilus::val<bool>>(static_cast<ExecutableDataType<ValueType>>(rightExp).rawValue);
    const auto resultIsNull = null || rightExp.isNull();
    return ExecutableDataType<bool>::create(rawValueBool && otherRawValueBool, resultIsNull);
}

template<typename ValueType>
ExecDataType ExecutableDataType<ValueType>::operator||(const AbstractDataType& rightExp) const {
    const auto rawValueBool = static_cast<nautilus::val<bool>>(rawValue);
    const auto otherRawValueBool = static_cast<nautilus::val<bool>>(static_cast<ExecutableDataType<ValueType>>(rightExp).rawValue);
    const auto resultIsNull = null || rightExp.isNull();
    return ExecutableDataType<bool>::create(rawValueBool || otherRawValueBool, resultIsNull);
}

template<typename ValueType>
ExecDataType ExecutableDataType<ValueType>::operator==(const AbstractDataType& rightExp) const {
    const auto otherRawValue = static_cast<ExecutableDataType<ValueType>>(rightExp).rawValue;
    const auto resultIsNull = null || rightExp.isNull();
    return ExecutableDataType<bool>::create(rawValue == otherRawValue, resultIsNull);
}

template<typename ValueType>
ExecDataType ExecutableDataType<ValueType>::operator!=(const AbstractDataType& rightExp) const {
    return !(this->operator==(rightExp));
}

template<typename ValueType>
ExecDataType ExecutableDataType<ValueType>::operator<(const AbstractDataType& rightExp) const {
    const auto otherRawValue = static_cast<ExecutableDataType<ValueType>>(rightExp).rawValue;
    const auto resultIsNull = null || rightExp.isNull();
    return ExecutableDataType<bool>::create(rawValue < otherRawValue, resultIsNull);
}

template<typename ValueType>
ExecDataType ExecutableDataType<ValueType>::operator>(const AbstractDataType& rightExp) const {
    const auto otherRawValue = static_cast<ExecutableDataType<ValueType>>(rightExp).rawValue;
    const auto resultIsNull = null || rightExp.isNull();
    return ExecutableDataType<bool>::create(rawValue > otherRawValue, resultIsNull);
}

template<typename ValueType>
ExecDataType ExecutableDataType<ValueType>::operator<=(const AbstractDataType& rightExp) const {
    const auto otherRawValue = static_cast<ExecutableDataType<ValueType>>(rightExp).rawValue;
    const auto resultIsNull = null || rightExp.isNull();
    return ExecutableDataType<bool>::create(rawValue <= otherRawValue, resultIsNull);
}

template<typename ValueType>
ExecDataType ExecutableDataType<ValueType>::operator>=(const AbstractDataType& rightExp) const {
    const auto otherRawValue = static_cast<ExecutableDataType<ValueType>>(rightExp).rawValue;
    const auto resultIsNull = null || rightExp.isNull();
    return ExecutableDataType<bool>::create(rawValue >= otherRawValue, resultIsNull);
}

template<typename ValueType>
ExecDataType ExecutableDataType<ValueType>::operator+(const AbstractDataType& rightExp) const {
    const auto otherRawValue = static_cast<ExecutableDataType<ValueType>>(rightExp).rawValue;
    const auto resultIsNull = null || rightExp.isNull();
    return ExecutableDataType<ValueType>::create(rawValue + otherRawValue, resultIsNull);
}

template<typename ValueType>
ExecDataType ExecutableDataType<ValueType>::operator-(const AbstractDataType& rightExp) const {
    const auto otherRawValue = static_cast<ExecutableDataType<ValueType>>(rightExp).rawValue;
    const auto resultIsNull = null || rightExp.isNull();
    return ExecutableDataType<ValueType>::create(rawValue - otherRawValue, resultIsNull);
}

template<typename ValueType>
ExecDataType ExecutableDataType<ValueType>::operator*(const AbstractDataType& rightExp) const {
    const auto otherRawValue = static_cast<ExecutableDataType<ValueType>>(rightExp).rawValue;
    const auto resultIsNull = null || rightExp.isNull();
    return ExecutableDataType<ValueType>::create(rawValue * otherRawValue, resultIsNull);
}

template<typename ValueType>
ExecDataType ExecutableDataType<ValueType>::operator/(const AbstractDataType& rightExp) const {
    const auto otherRawValue = static_cast<ExecutableDataType<ValueType>>(rightExp).rawValue;
    const auto resultIsNull = null || rightExp.isNull();
    return ExecutableDataType<ValueType>::create(rawValue / otherRawValue, resultIsNull);
}

template<typename ValueType>
ExecDataType ExecutableDataType<ValueType>::operator%(const AbstractDataType& rightExp) const {
    const auto otherRawValue = static_cast<ExecutableDataType<ValueType>>(rightExp).rawValue;
    const auto resultIsNull = null || rightExp.isNull();
    return ExecutableDataType<ValueType>::create(rawValue % otherRawValue, resultIsNull);
}

template<typename ValueType>
ExecDataType ExecutableDataType<ValueType>::operator&(const AbstractDataType& rightExp) const {
    const auto otherRawValue = static_cast<ExecutableDataType<ValueType>>(rightExp).rawValue;
    const auto resultIsNull = null || rightExp.isNull();
    return ExecutableDataType<ValueType>::create(rawValue & otherRawValue, resultIsNull);
}

template<typename ValueType>
ExecDataType ExecutableDataType<ValueType>::operator|(const AbstractDataType& rightExp) const {
    const auto otherRawValue = static_cast<ExecutableDataType<ValueType>>(rightExp).rawValue;
    const auto resultIsNull = null || rightExp.isNull();
    return ExecutableDataType<ValueType>::create(rawValue | otherRawValue, resultIsNull);
}

template<typename ValueType>
ExecDataType ExecutableDataType<ValueType>::operator^(const AbstractDataType& rightExp) const {
    const auto otherRawValue = static_cast<ExecutableDataType<ValueType>>(rightExp).rawValue;
    const auto resultIsNull = null || rightExp.isNull();
    return ExecutableDataType<ValueType>::create(rawValue ^ otherRawValue, resultIsNull);
}

template<typename ValueType>
ExecDataType ExecutableDataType<ValueType>::operator<<(const AbstractDataType& rightExp) const {
    const auto otherRawValue = static_cast<ExecutableDataType<ValueType>>(rightExp).rawValue;
    const auto resultIsNull = null || rightExp.isNull();
    return ExecutableDataType<ValueType>::create(rawValue << otherRawValue, resultIsNull);
}

template<typename ValueType>
ExecDataType ExecutableDataType<ValueType>::operator>>(const AbstractDataType& rightExp) const {
    const auto otherRawValue = static_cast<ExecutableDataType<ValueType>>(rightExp).rawValue;
    const auto resultIsNull = null || rightExp.isNull();
    return ExecutableDataType<ValueType>::create(rawValue >> otherRawValue, resultIsNull);
}

template<typename ValueType>
ExecDataType ExecutableDataType<ValueType>::operator!() const {
    const auto resultIsNull = null;
    return ExecutableDataType<bool>::create(!rawValue, resultIsNull);
}

}
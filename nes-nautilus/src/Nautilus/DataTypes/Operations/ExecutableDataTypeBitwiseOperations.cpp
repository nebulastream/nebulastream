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

#include <Nautilus/DataTypes/Operations/ExecutableDataTypeBitwiseOperations.hpp>


namespace NES::Nautilus {

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
    if (lhsCasted->instanceOf<FixedSizeExecutableDataType<UInt8>>() || lhsCasted->instanceOf<FixedSizeExecutableDataType<Int8>>() || lhsCasted->instanceOf<FixedSizeExecutableDataType<UInt16>>()
        || lhsCasted->instanceOf<FixedSizeExecutableDataType<Int16>>()) {
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
    if (lhsCasted->instanceOf<FixedSizeExecutableDataType<UInt8>>() || lhsCasted->instanceOf<FixedSizeExecutableDataType<Int8>>() || lhsCasted->instanceOf<FixedSizeExecutableDataType<UInt16>>()
        || lhsCasted->instanceOf<FixedSizeExecutableDataType<Int16>>()) {
        const auto lhsCastedInt = castTo<int32_t>(lhsCasted);
        const auto rhsCastedInt = castTo<int32_t>(rhsCasted);
        return *lhsCastedInt >> rhsCastedInt;
    }

    return *lhsCasted >> rhsCasted;
}

}// namespace NES::Nautilus

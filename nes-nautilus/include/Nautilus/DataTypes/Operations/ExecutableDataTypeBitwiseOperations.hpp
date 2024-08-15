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

#ifndef NES_NAUTILUS_INCLUDE_NAUTILUS_DATATYPES_EXECUTABLEDATATYPEBITWISEOPERATIONS_HPP_
#define NES_NAUTILUS_INCLUDE_NAUTILUS_DATATYPES_EXECUTABLEDATATYPEBITWISEOPERATIONS_HPP_

#include <Nautilus/DataTypes/AbstractDataType.hpp>
#include <Nautilus/DataTypes/ExecutableDataType.hpp>
#include <Nautilus/DataTypes/ExecutableDataTypesConcepts.hpp>

namespace NES::Nautilus {

/// Bitwise And
template<ConstantAndFixedDataTypes RHS>
ExecDataType operator&(const ExecDataType& lhs, const RHS& rhs) {
    return lhs & nautilus::val<RHS>(rhs);
}

template<ConstantAndFixedDataTypes LHS>
ExecDataType operator&(const LHS& lhs, const ExecDataType& rhs) {
    return nautilus::val<LHS>(lhs) & rhs;
}

template<FixedDataTypes RHS>
ExecDataType operator&(const ExecDataType& lhs, const nautilus::val<RHS>& rhs) {
    return lhs & ExecutableDataType<RHS>::create(rhs);
}

template<FixedDataTypes LHS>
ExecDataType operator&(const nautilus::val<LHS>& lhs, const ExecDataType& rhs) {
    return ExecutableDataType<LHS>::create(lhs) & rhs;
}

ExecDataType operator&(const ExecDataType& lhs, const ExecDataType& rhs);

/// Bitwise Or
template<ConstantAndFixedDataTypes RHS>
ExecDataType operator|(const ExecDataType& lhs, const RHS& rhs) {
    return lhs | nautilus::val<RHS>(rhs);
}

template<ConstantAndFixedDataTypes LHS>
ExecDataType operator|(const LHS& lhs, const ExecDataType& rhs) {
    return nautilus::val<LHS>(lhs) | rhs;
}

template<FixedDataTypes RHS>
ExecDataType operator|(const ExecDataType& lhs, const nautilus::val<RHS>& rhs) {
    return lhs | ExecutableDataType<RHS>::create(rhs);
}

template<FixedDataTypes LHS>
ExecDataType operator|(const nautilus::val<LHS>& lhs, const ExecDataType& rhs) {
    return ExecutableDataType<LHS>::create(lhs) | rhs;
}

ExecDataType operator|(const ExecDataType& lhs, const ExecDataType& rhs);

/// Bitwise Xor
template<ConstantAndFixedDataTypes RHS>
ExecDataType operator^(const ExecDataType& lhs, const RHS& rhs) {
    return lhs ^ nautilus::val<RHS>(rhs);
}

template<ConstantAndFixedDataTypes LHS>
ExecDataType operator^(const LHS& lhs, const ExecDataType& rhs) {
    return nautilus::val<LHS>(lhs) ^ rhs;
}

template<FixedDataTypes RHS>
ExecDataType operator^(const ExecDataType& lhs, const nautilus::val<RHS>& rhs) {
    return lhs ^ ExecutableDataType<RHS>::create(rhs);
}

template<FixedDataTypes LHS>
ExecDataType operator^(const nautilus::val<LHS>& lhs, const ExecDataType& rhs) {
    return ExecutableDataType<LHS>::create(lhs) ^ rhs;
}

ExecDataType operator^(const ExecDataType& lhs, const ExecDataType& rhs);

/// Bitwise Left Shift
template<ConstantAndFixedDataTypes RHS>
ExecDataType operator<<(const ExecDataType& lhs, const RHS& rhs) {
    return lhs << nautilus::val<RHS>(rhs);
}

template<ConstantAndFixedDataTypes LHS>
ExecDataType operator<<(const LHS& lhs, const ExecDataType& rhs) {
    return nautilus::val<LHS>(lhs) << rhs;
}

template<FixedDataTypes RHS>
ExecDataType operator<<(const ExecDataType& lhs, const nautilus::val<RHS>& rhs) {
    return lhs << ExecutableDataType<RHS>::create(rhs);
}

template<FixedDataTypes LHS>
ExecDataType operator<<(const nautilus::val<LHS>& lhs, const ExecDataType& rhs) {
    return ExecutableDataType<LHS>::create(lhs) << rhs;
}

ExecDataType operator<<(const ExecDataType& lhs, const ExecDataType& rhs);

/// Bitwise Right Shift
template<ConstantAndFixedDataTypes RHS>
ExecDataType operator>>(const ExecDataType& lhs, const RHS& rhs) {
    return lhs >> nautilus::val<RHS>(rhs);
}

template<ConstantAndFixedDataTypes LHS>
ExecDataType operator>>(const LHS& lhs, const ExecDataType& rhs) {
    return nautilus::val<LHS>(lhs) >> rhs;
}

template<FixedDataTypes RHS>
ExecDataType operator>>(const ExecDataType& lhs, const nautilus::val<RHS>& rhs) {
    return lhs >> ExecutableDataType<RHS>::create(rhs);
}

template<FixedDataTypes LHS>
ExecDataType operator>>(const nautilus::val<LHS>& lhs, const ExecDataType& rhs) {
    return ExecutableDataType<LHS>::create(lhs) >> rhs;
}

ExecDataType operator>>(const ExecDataType& lhs, const ExecDataType& rhs);

}// namespace NES::Nautilus

#endif// NES_NAUTILUS_INCLUDE_NAUTILUS_DATATYPES_EXECUTABLEDATATYPEBITWISEOPERATIONS_HPP_

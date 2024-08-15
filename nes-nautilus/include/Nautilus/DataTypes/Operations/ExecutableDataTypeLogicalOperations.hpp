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

#ifndef NES_NAUTILUS_INCLUDE_NAUTILUS_DATATYPES_EXECUTABLEDATATYPELOGICALOPERATIONS_HPP_
#define NES_NAUTILUS_INCLUDE_NAUTILUS_DATATYPES_EXECUTABLEDATATYPELOGICALOPERATIONS_HPP_

#include <Nautilus/DataTypes/AbstractDataType.hpp>
#include <Nautilus/DataTypes/FixedSizeExecutableDataType.hpp>
#include <Nautilus/DataTypes/ExecutableDataTypesConcepts.hpp>

namespace NES::Nautilus {

/// Logical And
template<ConstantAndFixedDataTypes RHS>
ExecDataType operator&&(const ExecDataType& lhs, const RHS& rhs) {
    return lhs && nautilus::val<RHS>(rhs);
}

template<ConstantAndFixedDataTypes LHS>
ExecDataType operator&&(const LHS& lhs, const ExecDataType& rhs) {
    return nautilus::val<LHS>(lhs) && rhs;
}

template<FixedDataTypes RHS>
ExecDataType operator&&(const ExecDataType& lhs, const nautilus::val<RHS>& rhs) {
    return lhs && FixedSizeExecutableDataType<RHS>::create(rhs);
}
template<FixedDataTypes LHS>
ExecDataType operator&&(const nautilus::val<LHS>& lhs, const ExecDataType& rhs) {
    return FixedSizeExecutableDataType<LHS>::create(lhs) && rhs;
}

ExecDataType operator&&(const ExecDataType& lhs, const ExecDataType& rhs);


/// Logical Or
template<ConstantAndFixedDataTypes RHS>
ExecDataType operator||(const ExecDataType& lhs, const RHS& rhs) {
    return lhs || nautilus::val<RHS>(rhs);
}

template<ConstantAndFixedDataTypes LHS>
ExecDataType operator||(const LHS& lhs, const ExecDataType& rhs) {
    return nautilus::val<LHS>(lhs) || rhs;
}

template<FixedDataTypes RHS>
ExecDataType operator||(const ExecDataType& lhs, const nautilus::val<RHS>& rhs) {
    return lhs || FixedSizeExecutableDataType<RHS>::create(rhs);
}
template<FixedDataTypes LHS>
ExecDataType operator||(const nautilus::val<LHS>& lhs, const ExecDataType& rhs) {
    return FixedSizeExecutableDataType<LHS>::create(lhs) || rhs;
}

ExecDataType operator||(const ExecDataType& lhs, const ExecDataType& rhs);

/// Equals
template<ConstantAndFixedDataTypes RHS>
ExecDataType operator==(const ExecDataType& lhs, const RHS& rhs) {
    return lhs == nautilus::val<RHS>(rhs);
}

template<ConstantAndFixedDataTypes LHS>
ExecDataType operator==(const LHS& lhs, const ExecDataType& rhs) {
    return nautilus::val<LHS>(lhs) == rhs;
}

template<FixedDataTypes RHS>
ExecDataType operator==(const ExecDataType& lhs, const nautilus::val<RHS>& rhs) {
    return lhs == FixedSizeExecutableDataType<RHS>::create(rhs);
}
template<FixedDataTypes LHS>
ExecDataType operator==(const nautilus::val<LHS>& lhs, const ExecDataType& rhs) {
    return FixedSizeExecutableDataType<LHS>::create(lhs) == rhs;
}

ExecDataType operator==(const ExecDataType& lhs, const ExecDataType& rhs);

/// Not Equals
template<ConstantAndFixedDataTypes RHS>
ExecDataType operator!=(const ExecDataType& lhs, const RHS& rhs) {
    return lhs != nautilus::val<RHS>(rhs);
}

template<ConstantAndFixedDataTypes LHS>
ExecDataType operator!=(const LHS& lhs, const ExecDataType& rhs) {
    return nautilus::val<LHS>(lhs) != rhs;
}

template<FixedDataTypes RHS>
ExecDataType operator!=(const ExecDataType& lhs, const nautilus::val<RHS>& rhs) {
    return lhs != FixedSizeExecutableDataType<RHS>::create(rhs);
}
template<FixedDataTypes LHS>
ExecDataType operator!=(const nautilus::val<LHS>& lhs, const ExecDataType& rhs) {
    return FixedSizeExecutableDataType<LHS>::create(lhs) != rhs;
}

ExecDataType operator!=(const ExecDataType& lhs, const ExecDataType& rhs);

/// Less Than
template<ConstantAndFixedDataTypes RHS>
ExecDataType operator<(const ExecDataType& lhs, const RHS& rhs) {
    return lhs < nautilus::val<RHS>(rhs);
}

template<ConstantAndFixedDataTypes LHS>
ExecDataType operator<(const LHS& lhs, const ExecDataType& rhs) {
    return nautilus::val<LHS>(lhs) < rhs;
}

template<FixedDataTypes RHS>
ExecDataType operator<(const ExecDataType& lhs, const nautilus::val<RHS>& rhs) {
    return lhs < FixedSizeExecutableDataType<RHS>::create(rhs);
}
template<FixedDataTypes LHS>
ExecDataType operator<(const nautilus::val<LHS>& lhs, const ExecDataType& rhs) {
    return FixedSizeExecutableDataType<LHS>::create(lhs) < rhs;
}

ExecDataType operator<(const ExecDataType& lhs, const ExecDataType& rhs);

/// Greater Than
template<ConstantAndFixedDataTypes RHS>
ExecDataType operator>(const ExecDataType& lhs, const RHS& rhs) {
    return lhs > nautilus::val<RHS>(rhs);
}

template<ConstantAndFixedDataTypes LHS>
ExecDataType operator>(const LHS& lhs, const ExecDataType& rhs) {
    return nautilus::val<LHS>(lhs) > rhs;
}

template<FixedDataTypes RHS>
ExecDataType operator>(const ExecDataType& lhs, const nautilus::val<RHS>& rhs) {
    return lhs > FixedSizeExecutableDataType<RHS>::create(rhs);
}
template<FixedDataTypes LHS>
ExecDataType operator>(const nautilus::val<LHS>& lhs, const ExecDataType& rhs) {
    return FixedSizeExecutableDataType<LHS>::create(lhs) > rhs;
}

ExecDataType operator>(const ExecDataType& lhs, const ExecDataType& rhs);


/// Less Than or Equals
template<ConstantAndFixedDataTypes RHS>
ExecDataType operator<=(const ExecDataType& lhs, const RHS& rhs) {
    return lhs <= nautilus::val<RHS>(rhs);
}

template<ConstantAndFixedDataTypes LHS>
ExecDataType operator<=(const LHS& lhs, const ExecDataType& rhs) {
    return nautilus::val<LHS>(lhs) <= rhs;
}

template<FixedDataTypes RHS>
ExecDataType operator<=(const ExecDataType& lhs, const nautilus::val<RHS>& rhs) {
    return lhs <= FixedSizeExecutableDataType<RHS>::create(rhs);
}
template<FixedDataTypes LHS>
ExecDataType operator<=(const nautilus::val<LHS>& lhs, const ExecDataType& rhs) {
    return FixedSizeExecutableDataType<LHS>::create(lhs) <= rhs;
}

ExecDataType operator<=(const ExecDataType& lhs, const ExecDataType& rhs);

/// Greater Than or Equals
template<ConstantAndFixedDataTypes RHS>
ExecDataType operator>=(const ExecDataType& lhs, const RHS& rhs) {
    return lhs >= nautilus::val<RHS>(rhs);
}

template<ConstantAndFixedDataTypes LHS>
ExecDataType operator>=(const LHS& lhs, const ExecDataType& rhs) {
    return nautilus::val<LHS>(lhs) >= rhs;
}

template<FixedDataTypes RHS>
ExecDataType operator>=(const ExecDataType& lhs, const nautilus::val<RHS>& rhs) {
    return lhs >= FixedSizeExecutableDataType<RHS>::create(rhs);
}
template<FixedDataTypes LHS>
ExecDataType operator>=(const nautilus::val<LHS>& lhs, const ExecDataType& rhs) {
    return FixedSizeExecutableDataType<LHS>::create(lhs) >= rhs;
}

ExecDataType operator>=(const ExecDataType& lhs, const ExecDataType& rhs);


/// Logical not
ExecDataType operator!(const ExecDataType& lhs);

}// namespace NES::Nautilus

#endif// NES_NAUTILUS_INCLUDE_NAUTILUS_DATATYPES_EXECUTABLEDATATYPELOGICALOPERATIONS_HPP_

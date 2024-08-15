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
#include <Nautilus/DataTypes/VariableSizeExecutableDataType.hpp>
#include <nautilus/std/cstring.h>

namespace NES::Nautilus {
ExecDataType VariableSizeExecutableDataType::operator&&(const ExecDataType&) const { NES_NOT_IMPLEMENTED(); }
ExecDataType VariableSizeExecutableDataType::operator||(const ExecDataType&) const { NES_NOT_IMPLEMENTED(); }
ExecDataType VariableSizeExecutableDataType::operator<(const ExecDataType&) const { NES_NOT_IMPLEMENTED(); }
ExecDataType VariableSizeExecutableDataType::operator>(const ExecDataType&) const { NES_NOT_IMPLEMENTED(); }
ExecDataType VariableSizeExecutableDataType::operator<=(const ExecDataType&) const { NES_NOT_IMPLEMENTED(); }
ExecDataType VariableSizeExecutableDataType::operator>=(const ExecDataType&) const { NES_NOT_IMPLEMENTED(); }
ExecDataType VariableSizeExecutableDataType::operator+(const ExecDataType&) const { NES_NOT_IMPLEMENTED(); }
ExecDataType VariableSizeExecutableDataType::operator-(const ExecDataType&) const { NES_NOT_IMPLEMENTED(); }
ExecDataType VariableSizeExecutableDataType::operator*(const ExecDataType&) const { NES_NOT_IMPLEMENTED(); }
ExecDataType VariableSizeExecutableDataType::operator/(const ExecDataType&) const { NES_NOT_IMPLEMENTED(); }
ExecDataType VariableSizeExecutableDataType::operator%(const ExecDataType&) const { NES_NOT_IMPLEMENTED(); }
ExecDataType VariableSizeExecutableDataType::operator&(const ExecDataType&) const { NES_NOT_IMPLEMENTED(); }
ExecDataType VariableSizeExecutableDataType::operator|(const ExecDataType&) const { NES_NOT_IMPLEMENTED(); }
ExecDataType VariableSizeExecutableDataType::operator^(const ExecDataType&) const { NES_NOT_IMPLEMENTED(); }
ExecDataType VariableSizeExecutableDataType::operator<<(const ExecDataType&) const { NES_NOT_IMPLEMENTED(); }
ExecDataType VariableSizeExecutableDataType::operator>>(const ExecDataType&) const { NES_NOT_IMPLEMENTED(); }
ExecDataType VariableSizeExecutableDataType::operator!() const { NES_NOT_IMPLEMENTED(); }

ExecDataType VariableSizeExecutableDataType::operator==(const ExecDataType& rhs) const {
    if (!rhs->instanceOf<VariableSizeExecutableDataType>()) {
        NES_THROW_RUNTIME_ERROR("Can only compare VariableSizeExecutableDataType with VariableSizeExecutableDataType");
    }

    if (null || rhs->isNull()) {
        return FixedSizeExecutableDataType<bool>::create(false, true);
    }

    if (size != rhs->as<VariableSizeExecutableDataType>()->size) {
        return FixedSizeExecutableDataType<bool>::create(false, false);
    }

    const auto compareResult = (nautilus::memcmp(content, rhs->as<VariableSizeExecutableDataType>()->content, size) == 0);
    return FixedSizeExecutableDataType<bool>::create(compareResult, false);
}
ExecDataType VariableSizeExecutableDataType::operator!=(const ExecDataType& rhs) const {
    if (!rhs->instanceOf<VariableSizeExecutableDataType>()) {
        NES_THROW_RUNTIME_ERROR("Can only compare VariableSizeExecutableDataType with VariableSizeExecutableDataType");
    }
    return !(*this == rhs);
}

}// namespace NES::Nautilus
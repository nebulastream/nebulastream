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

#ifndef VariableSizedData_HPP
#define VariableSizedData_HPP

#include <Nautilus/DataTypes/DataTypesUtil.hpp>

#include <iomanip>

namespace NES::Nautilus {

// Declaring the class here, so that we can declare the operator==(const VariableSizedData, const BooleanVal) for it
class VariableSizedData;
BooleanVal operator==(const VariableSizedData& varSizedData, const BooleanVal& other);
BooleanVal operator==(const BooleanVal& other, const VariableSizedData& varSizedData);

/// We assume that the first 4 bytes of a int8_t* to any var sized data contains the length of the var sized data
class VariableSizedData {
  public:
    explicit VariableSizedData(const MemRefVal& content, const UInt32Val& size);
    explicit VariableSizedData(const MemRefVal& pointerToVarSized);
    VariableSizedData(const VariableSizedData& other);
    VariableSizedData& operator=(const VariableSizedData& other) noexcept;

    [[nodiscard]] UInt32Val getSize() const;
    [[nodiscard]] MemRefVal getContent() const;
    [[nodiscard]] MemRefVal getReference() const;
    std::ostream& operator<<(std::ostream& os) const;

    /// Declaring friend for it, so that we can call VariableSizedData::operator!() in it
    friend BooleanVal operator==(const VariableSizedData& varSizedData, const BooleanVal& other);
    friend BooleanVal operator==(const BooleanVal& other, const VariableSizedData& varSizedData);

    BooleanVal operator==(const VariableSizedData&) const;
    BooleanVal operator!=(const VariableSizedData&) const;
    BooleanVal operator!() const;

  protected:
    /// For now, we assume that the size and content is always fixed
    UInt32Val size;
    MemRefVal content;
};

BooleanVal operator!(const VariableSizedData& varSizedData);

}// namespace NES::Nautilus

#endif//VariableSizedData_HPP
